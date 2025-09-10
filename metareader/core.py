from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence, Union, Optional, Tuple, Callable, Any
import pandas as pd
import numpy as np
import re, warnings, shutil, tempfile, os, glob

# Optional fuzzy support (pip install "metareader[fuzzy]" -> rapidfuzz)
try:
    from rapidfuzz import process as rf_process, fuzz as rf_fuzz
    _RAPID = True
except Exception:
    _RAPID = False

__all__ = ["MetaReader", "resolve_data_path"]

# =========================
# Helpers
# =========================

def _norm(x):
    """Normalize cell/ID to a trimmed string, empty for NaN/None."""
    return ("" if x is None or (isinstance(x, float) and pd.isna(x)) else str(x).strip())

def _strip_units(text: str) -> str:
    """Remove bracketed units, e.g. 'Pressure [kPa]' -> 'Pressure', 'Temp (°C)' -> 'Temp'"""
    return re.sub(r"[\[\(].*?[\]\)]", "", str(text)).strip()

def _as_list(x) -> List:
    if x is None: return []
    if isinstance(x, (list, tuple, set)): return list(x)
    return [x]

# --------- path resolution ---------
Resolver = Union[str, Path, Sequence[Union[str, Path]], Dict[str, Any]]
PickRule = str  # "exact" | "latest" | "largest"

def resolve_data_path(
    path: Resolver,
    *,
    pick: PickRule = "exact",
    default_glob: Optional[str] = None,
    on_empty: str = "error",  # "error" | "none"
    custom_picker: Optional[Callable[[Sequence[Path]], Path]] = None,
    path_loader: Optional[Callable[[str], Union[str, Path]]] = None,
    loader_registry: Optional[Dict[str, Callable[[str], Union[str, Path]]]] = None,
) -> Optional[Path]:
    """
    Resolve a path that could be:
      - exact str/Path,
      - a glob pattern (contains * or ?),
      - a list of paths,
      - a dict like {"root": "/data", "glob": "Results *.xlsx"}.

    Also supports "scheme://..." via a loader (path_loader or loader_registry[scheme]).
    Returns: resolved Path (or None if not found and on_empty='none').
    """
    def to_paths(x) -> Sequence[Path]:
        if isinstance(x, (str, Path)):
            return [Path(str(x))]
        return [Path(str(p)) for p in x]

    # Remote scheme loader?
    def _maybe_load_remote(pstr: str) -> Optional[Path]:
        if "://" not in pstr:
            return None
        scheme = pstr.split("://", 1)[0].lower()
        if path_loader:
            local = path_loader(pstr)
            return Path(str(local))
        if loader_registry and scheme in loader_registry:
            local = loader_registry[scheme](pstr)
            return Path(str(local))
        raise ValueError(f"No loader available for scheme '{scheme}' in path '{pstr}'")

    candidates: Sequence[Path] = []

    if isinstance(path, dict):
        root = Path(path.get("root", "."))
        gpat = path.get("glob") or default_glob or "*"
        candidates = [Path(p) for p in glob.glob(str(root / gpat))]
    elif isinstance(path, (list, tuple)):
        candidates = to_paths(path)
    else:
        p = str(path)
        # remote
        if "://" in p:
            local = _maybe_load_remote(p)
            candidates = [local] if local else []
        elif any(ch in p for ch in ["*", "?"]):
            candidates = [Path(x) for x in glob.glob(p)]
        else:
            candidates = [Path(p)]

    existing = [c for c in candidates if c and c.exists() and c.is_file()]
    if not existing:
        if on_empty == "none":
            return None
        raise FileNotFoundError(f"No matching files for: {path}")

    if custom_picker:
        return custom_picker(existing)

    pick = pick.lower()
    if pick == "exact":
        return existing[0]
    if pick == "latest":
        return max(existing, key=lambda p: p.stat().st_mtime)
    if pick == "largest":
        return max(existing, key=lambda p: p.stat().st_size)

    raise ValueError(f"Unknown pick rule: {pick}")

# =========================
# Per-sheet state
# =========================

@dataclass
class _SheetState:
    id_col: str
    raw_cols: List[str]                    # exact headers in file order
    base_cmp_cols: List[str]               # normalized headers for matching
    id_to_rows: Dict[str, np.ndarray]      # normalized ID -> row indices (np.array)
    ids_set: set                           # for existence checks
    col_cache: Dict[str, pd.Series]        # cached Series for requested columns (aligned to Excel row order)

# =========================
# MetaReader
# =========================

class MetaReader:
    """
    Fast, robust Excel metadata reader for big, frequently-updated files.

    - Constant-time ID lookups (per-sheet index).
    - Lazy I/O: reads ONLY the columns you request; caches columns until the file changes.
    - Works while the workbook is open in Excel (snapshots on lock).
    - Flexible plan + wildcard.
    - Match modes: contains / exact / regex / (optional) fuzzy.
    - Run from code, DataFrame, or a control sheet.
    """

    def __init__(self,
                 path: Union[str, Path, Sequence[Union[str, Path]], Dict[str, Any]],
                 plan: Optional[Dict[str, Union[str, Sequence[str]]]] = None,
                 default_id_candidates: Sequence[str] = ("Identifier", "ID", "Sample", "Name"),
                 ignore_units: bool = True,
                 case_insensitive: bool = True,
                 engine: str = "openpyxl",
                 snapshot_on_lock: bool = True,
                 snapshot_dir: Optional[Union[str, Path]] = None,
                 *,
                 # path features
                 path_pick: str = "exact",
                 path_default_glob: Optional[str] = None,
                 path_on_empty: str = "error",
                 path_custom_picker: Optional[Callable[[Sequence[Path]], Path]] = None,
                 path_loader: Optional[Callable[[str], Union[str, Path]]] = None,
                 loader_registry: Optional[Dict[str, Callable[[str], Union[str, Path]]]] = None,
                 # fuzzy & semantic
                 enable_fuzzy: bool = False,
                 fuzzy_score_cutoff: int = 82,
                 semantic_matcher: Optional[Callable[[str, Sequence[str]], Sequence[Tuple[int, float]]]] = None
                 ):
        """
        path: string, glob, list, or {"root": "...", "glob": "..."}.
              Remote like "s3://..." supported via loader hooks.
        enable_fuzzy: use RapidFuzz (if installed) to guess columns/IDs on typos.
        semantic_matcher: optional callable(term, candidates)-> list[(index, score)], higher=better.
        """
        # Resolve path to a concrete single file (MetaReader is single-file; see MultiMetaReader for many files)
        resolved = resolve_data_path(
            path,
            pick=path_pick,
            default_glob=path_default_glob,
            on_empty=path_on_empty,
            custom_picker=path_custom_picker,
            path_loader=path_loader,
            loader_registry=loader_registry,
        )
        if resolved is None:
            raise FileNotFoundError("No file resolved for MetaReader")
        self._raw_path = path
        self.path = str(resolved)

        self.engine = engine
        self.ignore_units = ignore_units
        self.case_insensitive = case_insensitive
        self.snapshot_on_lock = snapshot_on_lock
        self.snapshot_dir = str(snapshot_dir) if snapshot_dir else None

        self.enable_fuzzy = bool(enable_fuzzy and _RAPID)
        self.fuzzy_score_cutoff = int(fuzzy_score_cutoff)
        self.semantic_matcher = semantic_matcher  # optional

        self._mtime: Optional[float] = None
        self._sheets: Dict[str, _SheetState] = {}

        if plan is None:
            plan = {"*": list(default_id_candidates)}
        self.plan_raw = dict(plan)
        self.default_candidates = list(default_id_candidates)

        self.plan = self._expand_plan(self.path, self.plan_raw)
        self._ensure_all_initialized()

    # ------------- public API -------------

    def refresh(self, force: bool = False):
        """Rebuild indices if the file changed (or always if force=True)."""
        m = self._file_mtime()
        if (m != self._mtime) or force:
            self._mtime = m
            self._sheets = {sh: self._build_sheet_state(sh, idc) for sh, idc in self.plan.items()}

    def list_sheets(self) -> List[str]:
        return list(self.plan.keys())

    def list_columns(self, sheet: str) -> List[str]:
        self._ensure_sheet(sheet)
        return list(self._sheets[sheet].raw_cols)

    def find_columns(self, sheet: str, contains: Optional[str]=None, regex: Optional[str]=None) -> List[str]:
        self._ensure_sheet(sheet)
        st = self._sheets[sheet]
        cols = st.raw_cols
        base = [_strip_units(c) if self.ignore_units else c for c in cols]
        if self.case_insensitive:
            base = [c.lower() for c in base]
        if contains:
            key = contains.lower() if self.case_insensitive else contains
            return [cols[i] for i,c in enumerate(base) if key in c]
        if regex:
            flags = re.I if self.case_insensitive else 0
            rx = re.compile(regex, flags)
            return [cols[i] for i,c in enumerate(base) if rx.search(c)]
        return cols

    def preload_columns(self, sheet: str, variables: Sequence[str],
                        match_contains=True, match_exact=False, match_regex: Optional[str]=None) -> List[str]:
        self._refresh_if_changed()
        self._ensure_sheet(sheet)
        st = self._sheets[sheet]
        match_map = self._match_columns(st, list(variables),
                                        contains=(match_regex is None and not match_exact and match_contains),
                                        exact=match_exact,
                                        regex=match_regex)
        needed = {st.raw_cols[j] for js in match_map.values() for j in js}
        missing = [h for h in needed if h not in st.col_cache]
        if missing:
            df = self._read_excel(sheet_name=sheet, usecols=[st.id_col] + missing)
            for c in missing:
                st.col_cache[c] = df[c] if c in df.columns else pd.Series([np.nan]*len(df), dtype="object")
        return sorted(needed)

    def get(self,
            sheet: str,
            id_values: Union[str, Sequence[str]],
            variables: Union[str, Sequence[str]],
            match_contains: bool = True,
            match_exact: bool = False,
            match_regex: Optional[str] = None,
            *,
            match_fuzzy: Optional[bool] = None) -> pd.DataFrame:
        """
        Query a single sheet. Returns tidy DataFrame:
        Sheet, Identifier, Variable_Requested, Matched_Column, Value, Status
        """
        self._refresh_if_changed()
        self._ensure_sheet(sheet)

        st = self._sheets[sheet]
        req_ids  = [_norm(x) for x in _as_list(id_values)]
        req_vars = _as_list(variables)
        use_fuzzy = self.enable_fuzzy if match_fuzzy is None else bool(match_fuzzy and _RAPID)

        # columns to read
        match_map = self._match_columns(st, req_vars,
                                        contains=(match_regex is None and not match_exact and match_contains),
                                        exact=match_exact,
                                        regex=match_regex,
                                        use_fuzzy=use_fuzzy)
        needed = {st.raw_cols[j] for js in match_map.values() for j in js}
        need_read = [c for c in needed if c not in st.col_cache]
        if need_read:
            df = self._read_excel(sheet_name=sheet, usecols=[st.id_col] + need_read)
            for c in need_read:
                st.col_cache[c] = df.get(c, pd.Series([np.nan]*len(df), dtype="object"))

        # maybe resolve fuzzy IDs
        effective_ids: Dict[str, Tuple[str, str]] = {}  # requested -> (used, status_note)
        for rid in req_ids:
            if rid in st.id_to_rows:
                effective_ids[rid] = (rid, "")
            elif use_fuzzy and st.ids_set:
                best = self._best_fuzzy(rid, list(st.ids_set))
                if best and best[1] >= self.fuzzy_score_cutoff:
                    effective_ids[rid] = (best[0], f"ID_FUZZY->{best[0]}")
                else:
                    effective_ids[rid] = (rid, "")
            else:
                effective_ids[rid] = (rid, "")

        rows = []
        for req in req_vars:
            idxs = match_map.get(req, [])
            if not idxs:
                for rid in req_ids:
                    status = "ID_NOT_FOUND" if effective_ids[rid][0] not in st.id_to_rows else "VAR_NOT_FOUND"
                    note = effective_ids[rid][1]
                    if note:
                        status += "|" + note
                    rows.append({"Sheet": sheet, "Identifier": rid, "Variable_Requested": req,
                                 "Matched_Column": None, "Value": np.nan, "Status": status})
                continue

            for j in idxs:
                col_name = st.raw_cols[j]
                ser = st.col_cache[col_name]
                for rid in req_ids:
                    used_id, note = effective_ids[rid]
                    pos = st.id_to_rows.get(used_id)
                    if pos is None or len(pos) == 0:
                        status = "ID_NOT_FOUND"
                        if note: status += "|" + note
                        rows.append({"Sheet": sheet, "Identifier": rid, "Variable_Requested": req,
                                     "Matched_Column": col_name, "Value": np.nan, "Status": status})
                        continue
                    vals = ser.iloc[pos] if len(ser) > 0 else pd.Series([], dtype=float)
                    if vals.empty:
                        status = "EMPTY"
                        if note: status += "|" + note
                        rows.append({"Sheet": sheet, "Identifier": rid, "Variable_Requested": req,
                                     "Matched_Column": col_name, "Value": np.nan, "Status": status})
                    else:
                        for v in vals:
                            status = ("OK" if not pd.isna(v) else "EMPTY")
                            if note: status += "|" + note
                            rows.append({"Sheet": sheet, "Identifier": rid, "Variable_Requested": req,
                                         "Matched_Column": col_name, "Value": v, "Status": status})
        if not rows:
            return pd.DataFrame(columns=["Sheet","Identifier","Variable_Requested","Matched_Column","Value","Status"])
        return pd.DataFrame(rows)

    def get_many(self,
                 queries: Sequence[Tuple[str, Sequence[str], Sequence[str]]],
                 match_contains: bool = True,
                 match_exact: bool = False,
                 match_regex: Optional[str] = None,
                 *,
                 match_fuzzy: Optional[bool] = None) -> pd.DataFrame:
        frames = []
        for sheet, ids, vars_ in queries:
            frames.append(self.get(sheet, ids, vars_,
                                   match_contains=match_contains,
                                   match_exact=match_exact,
                                   match_regex=match_regex,
                                   match_fuzzy=match_fuzzy))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def get_from_df(self, query_df: pd.DataFrame,
                    sheet_col="sheet", ids_col="id_values", vars_col="vars",
                    sep=";|,",
                    match_contains=True, match_exact=False, match_regex: Optional[str]=None,
                    *, match_fuzzy: Optional[bool] = None) -> pd.DataFrame:
        qlist = []
        for _, row in query_df.iterrows():
            sh = str(row[sheet_col]).strip()
            ids = row[ids_col]; vs = row[vars_col]
            if isinstance(ids, (list, tuple, set)): ids_list = list(ids)
            else: ids_list = [i.strip() for i in re.split(sep, str(ids)) if str(i).strip()] if pd.notna(ids) else []
            if isinstance(vs, (list, tuple, set)): vars_list = list(vs)
            else: vars_list = [v.strip() for v in re.split(sep, str(vs)) if str(v).strip()] if pd.notna(vs) else []
            qlist.append((sh, ids_list, vars_list))
        return self.get_many(qlist, match_contains=match_contains, match_exact=match_exact,
                             match_regex=match_regex, match_fuzzy=match_fuzzy)

    def run_from_query_sheet(self, query_sheet="__Queries",
                             sheet_col="sheet", ids_col="id_values", vars_col="vars",
                             sep=";|,",
                             match_contains=True, match_exact=False, match_regex: Optional[str]=None,
                             *, match_fuzzy: Optional[bool] = None) -> pd.DataFrame:
        dfq = self._read_excel(sheet_name=query_sheet)
        if "match_contains" in dfq.columns:
            frames = []
            for _, r in dfq.iterrows():
                mc = bool(r.get("match_contains", True))
                frames.append(self.get_from_df(pd.DataFrame([r]), sheet_col, ids_col, vars_col, sep,
                                               match_contains=mc, match_exact=match_exact, match_regex=match_regex,
                                               match_fuzzy=match_fuzzy))
            return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        else:
            return self.get_from_df(dfq, sheet_col, ids_col, vars_col, sep,
                                    match_contains=match_contains, match_exact=match_exact,
                                    match_regex=match_regex, match_fuzzy=match_fuzzy)

    def get_all(self,
                sheets: Union[str, Sequence[str]] = "*",
                variables: Optional[Sequence[str]] = None,
                include_all_columns: bool = False,
                match_contains: bool = True,
                match_exact: bool = False,
                match_regex: Optional[str] = None,
                *, match_fuzzy: Optional[bool] = None) -> pd.DataFrame:
        if sheets == "*":
            sheet_list = list(self.plan.keys())
        else:
            sheet_list = _as_list(sheets)

        frames = []
        for sh in sheet_list:
            self._refresh_if_changed()
            self._ensure_sheet(sh)
            st = self._sheets[sh]

            if include_all_columns and variables is None:
                wdf = self._read_excel(sheet_name=sh)
                ids_ser = wdf.get(st.id_col, pd.Series([np.nan]*len(wdf)))
                for col in wdf.columns:
                    if col == st.id_col: continue
                    frames.append(pd.DataFrame({
                        "Sheet": sh,
                        "Identifier": ids_ser.astype(object).map(_norm),
                        "Variable_Requested": col,
                        "Matched_Column": col,
                        "Value": wdf[col],
                        "Status": ["OK" if not pd.isna(v) else "EMPTY" for v in wdf[col]]
                    }))
                continue

            if variables is None:
                req_vars = [c for c in st.raw_cols if c != st.id_col]
                contains = False; exact = True; rgx = None
            else:
                req_vars = list(variables)
                contains = match_contains; exact = match_exact; rgx = match_regex

            frames.append(
                self.get(sh, id_values=list(st.ids_set), variables=req_vars,
                         match_contains=contains, match_exact=exact, match_regex=rgx,
                         match_fuzzy=match_fuzzy)
            )
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def to_csv(self, df: pd.DataFrame, path: Union[str, Path]):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)

    def to_parquet(self, df: pd.DataFrame, path: Union[str, Path]):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)

    def clear_cache(self, sheet: Optional[str] = None):
        if sheet is None:
            for st in self._sheets.values():
                st.col_cache.clear()
        else:
            if sheet in self._sheets:
                self._sheets[sheet].col_cache.clear()

    # ------------- internals -------------

    def _expand_plan(self, path: str, plan_raw: Dict[str, Union[str, Sequence[str]]]) -> Dict[str, str]:
        xls = pd.ExcelFile(path, engine=self.engine)
        plan: Dict[str, str] = {}
        default = plan_raw.get("*", None) or self.default_candidates

        def choose_id_col(sheet: str, candidates: Union[str, Sequence[str]]) -> Optional[str]:
            hdr = pd.read_excel(path, sheet_name=sheet, nrows=0, engine=self.engine).columns.tolist()
            if isinstance(candidates, str):
                return candidates if candidates in hdr else None
            for cand in candidates:
                if cand in hdr: return cand
            return None

        if "*" in plan_raw:
            for sh in xls.sheet_names:
                chosen = choose_id_col(sh, plan_raw["*"])
                if chosen: plan[sh] = chosen
        else:
            for sh in xls.sheet_names:
                chosen = choose_id_col(sh, default)
                if chosen: plan[sh] = chosen

        for sh, idc in plan_raw.items():
            if sh == "*": continue
            chosen = choose_id_col(sh, idc)
            if chosen is None:
                warnings.warn(f"[plan] Sheet '{sh}' has no matching id column among {idc}; skipping override.")
                continue
            plan[sh] = chosen

        if not plan:
            raise ValueError("No valid sheets/id columns found. Check your workbook and plan.")
        return plan

    def _file_mtime(self) -> float:
        try:
            return Path(self.path).stat().st_mtime
        except FileNotFoundError:
            return -1.0

    def _ensure_all_initialized(self):
        self._mtime = self._file_mtime()
        for sh, id_col in self.plan.items():
            if sh not in self._sheets:
                self._sheets[sh] = self._build_sheet_state(sh, id_col)

    def _refresh_if_changed(self):
        m = self._file_mtime()
        if m != self._mtime:
            self._mtime = m
            self._sheets = {sh: self._build_sheet_state(sh, idc) for sh, idc in self.plan.items()}

    def _ensure_sheet(self, sheet: str):
        if sheet not in self._sheets:
            if sheet not in self.plan:
                raise KeyError(f"Sheet '{sheet}' not in plan. Add it to plan with its id_col.")
            self._sheets[sheet] = self._build_sheet_state(sheet, self.plan[sheet])

    # ---- robust read_excel with snapshot fallback on lock ----
    def _read_excel(self, **kwargs) -> pd.DataFrame:
        try:
            return pd.read_excel(self.path, engine=self.engine, **kwargs)
        except Exception:
            if not self.snapshot_on_lock:
                raise
            tmp_dir = self.snapshot_dir or tempfile.gettempdir()
            Path(tmp_dir).mkdir(parents=True, exist_ok=True)
            base = Path(self.path).name
            tmp_path = os.path.join(tmp_dir, f"MetaReader_snapshot_{base}")
            shutil.copy2(self.path, tmp_path)
            try:
                return pd.read_excel(tmp_path, engine=self.engine, **kwargs)
            finally:
                try: os.remove(tmp_path)
                except OSError: pass

    def _build_sheet_state(self, sheet: str, id_col: str) -> _SheetState:
        hdr = self._read_excel(sheet_name=sheet, nrows=0)
        raw_cols = list(hdr.columns)
        if id_col not in raw_cols:
            raise KeyError(f"[{sheet}] ID column '{id_col}' not found. Available: {raw_cols}")

        base_cols = [_strip_units(c) if self.ignore_units else str(c) for c in raw_cols]
        base_cmp  = [c.lower() for c in base_cols] if self.case_insensitive else base_cols

        id_df = self._read_excel(sheet_name=sheet, usecols=[id_col])
        ids_norm = id_df[id_col].astype("object").map(lambda x: _norm(x) or None)

        id_to_rows: Dict[str, List[int]] = {}
        for i, v in enumerate(ids_norm):
            if not v: continue
            id_to_rows.setdefault(v, []).append(i)
        id_to_rows_arr = {k: np.asarray(v, dtype=int) for k, v in id_to_rows.items()}

        return _SheetState(
            id_col=id_col,
            raw_cols=raw_cols,
            base_cmp_cols=base_cmp,
            id_to_rows=id_to_rows_arr,
            ids_set=set(id_to_rows_arr.keys()),
            col_cache={}
        )

    def _match_columns(self,
                       st: _SheetState,
                       req_vars: List[str],
                       contains: bool,
                       exact: bool,
                       regex: Optional[str],
                       use_fuzzy: bool = False) -> Dict[str, List[int]]:
        out: Dict[str, List[int]] = {}
        rx = re.compile(regex, re.I if self.case_insensitive else 0) if regex else None
        for rv in req_vars:
            if rx is not None:
                idxs = [i for i, c in enumerate(st.base_cmp_cols) if rx.search(c)]
            else:
                key = _strip_units(rv) if self.ignore_units else str(rv)
                key_cmp = key.lower() if self.case_insensitive else key
                if exact:
                    idxs = [i for i, c in enumerate(st.base_cmp_cols) if key_cmp == c]
                elif contains:
                    idxs = [i for i, c in enumerate(st.base_cmp_cols) if key_cmp in c]
                else:
                    idxs = []
                # fuzzy fallback
                if not idxs and use_fuzzy and _RAPID and st.base_cmp_cols:
                    best = self._best_fuzzy(key_cmp, st.base_cmp_cols)
                    if best and best[1] >= self.fuzzy_score_cutoff:
                        idxs = [best[2]]  # index
                # semantic fallback
                if not idxs and self.semantic_matcher:
                    pairs = list(self.semantic_matcher(key_cmp, st.base_cmp_cols))  # [(idx, score)]
                    if pairs:
                        idxs = [max(pairs, key=lambda x: x[1])[0]]
            out[rv] = idxs
        return out

    def _best_fuzzy(self, query: str, choices: Sequence[str]) -> Optional[Tuple[str, int, int]]:
        """Return (choice, score, index) best match via RapidFuzz."""
        if not _RAPID or not choices:
            return None
        results = rf_process.extract(query, list(enumerate(choices)),
                                     scorer=rf_fuzz.WRatio, limit=1)
        if not results:
            return None
        (idx, choice), score, _ = results[0]
        return (choice, int(score), int(idx))
