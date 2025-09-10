from __future__ import annotations
from typing import Dict, Sequence, Optional, Union, List, Tuple, Any
from pathlib import Path
import json
import pandas as pd
from .core import MetaReader, resolve_data_path
from .multi import MultiMetaReader

__all__ = ["extract", "run_config"]

def extract(path: Union[str, Path, Sequence[Union[str, Path]], Dict[str, Any]],
            plan: Optional[Dict[str, Union[str, Sequence[str]]]] = None,
            queries: Optional[Sequence[Dict]] = None,
            *,
            default_id_candidates: Sequence[str] = ("Identifier","ID","Sample","Name"),
            ignore_units: bool = True,
            case_insensitive: bool = True,
            engine: str = "openpyxl",
            snapshot_on_lock: bool = True,
            snapshot_dir: Optional[Union[str, Path]] = None,
            query_sheet: Optional[str] = None,
            get_all: bool = False,
            get_all_sheets: Union[str, Sequence[str]] = "*",
            get_all_vars: Optional[Sequence[str]] = None,
            include_all_columns: bool = False,
            match_contains: bool = True,
            match_exact: bool = False,
            match_regex: Optional[str] = None,
            match_fuzzy: Optional[bool] = None,
            write_csv: Optional[Union[str, Path]] = None,
            write_parquet: Optional[Union[str, Path]] = None,
            return_df: bool = True,
            # path resolver & loader
            path_pick: str = "exact",
            path_default_glob: Optional[str] = None,
            path_loader=None,
            loader_registry=None,
            # fuzzy/semantic
            enable_fuzzy: bool = False,
            fuzzy_score_cutoff: int = 82,
            semantic_matcher=None
            ) -> Optional[pd.DataFrame]:
    """Convenience wrapper around MetaReader / MultiMetaReader.
       If 'path' resolves to multiple files, MultiMetaReader is used automatically."""
    # Collect candidates for multi if needed
    multi_candidates: List[Path] = []
    def _collect(p):
        rp = resolve_data_path(p, pick=path_pick, default_glob=path_default_glob,
                               on_empty="none", path_loader=path_loader, loader_registry=loader_registry)
        if rp: multi_candidates.append(rp)

    if isinstance(path, (list, tuple, set)):
        for p in path: _collect(p)
    elif isinstance(path, dict) or (isinstance(path, str) and any(ch in str(path) for ch in ["*", "?"])):
        gpat = None
        if isinstance(path, dict):
            root = Path(path.get("root", "."))
            gpat = str(root / (path.get("glob") or "*"))
        else:
            gpat = str(path)
        for p in Path().glob(gpat) if "://" not in gpat else []:
            _collect(str(p))

    if len(multi_candidates) > 1:
        reader = MultiMetaReader(
            [str(p) for p in multi_candidates], plan=plan, default_id_candidates=default_id_candidates,
            ignore_units=ignore_units, case_insensitive=case_insensitive, engine=engine,
            snapshot_on_lock=snapshot_on_lock, snapshot_dir=snapshot_dir,
            enable_fuzzy=enable_fuzzy or bool(match_fuzzy), fuzzy_score_cutoff=fuzzy_score_cutoff,
            semantic_matcher=semantic_matcher
        )
        def _finalize(df):
            if write_csv: reader.readers[0].to_csv(df, write_csv)
            if write_parquet: reader.readers[0].to_parquet(df, write_parquet)
            return df if return_df else None

        if get_all:
            df = reader.get_all(sheets=get_all_sheets, variables=get_all_vars,
                                include_all_columns=include_all_columns,
                                match_contains=match_contains, match_exact=match_exact,
                                match_regex=match_regex, match_fuzzy=match_fuzzy)
            return _finalize(df)
        elif query_sheet:
            df = reader.readers[0].run_from_query_sheet(query_sheet, match_contains=match_contains,
                                                        match_exact=match_exact, match_regex=match_regex,
                                                        match_fuzzy=match_fuzzy)
            if not df.empty:
                df.insert(0, "Source", Path(reader.readers[0].path).name)
            return _finalize(df)
        elif queries:
            norm = []
            for q in queries:
                sheet = q.get("sheet")
                ids = q.get("id_values") or q.get("ids") or []
                vars_ = q.get("vars") or q.get("variables") or []
                if isinstance(ids, str): ids = [s.strip() for s in ids.split(";") if s.strip()]
                if isinstance(vars_, str): vars_ = [s.strip() for s in vars_.split(";") if s.strip()]
                norm.append((sheet, ids, vars_))
            df = reader.get_many(norm, match_contains=match_contains, match_exact=match_exact,
                                 match_regex=match_regex, match_fuzzy=match_fuzzy)
            return _finalize(df)
        else:
            raise ValueError("Nothing to do: provide 'queries', or 'query_sheet', or set get_all=True.")
    else:
        reader = MetaReader(
            path=path, plan=plan or {"*": list(default_id_candidates)},
            default_id_candidates=default_id_candidates, ignore_units=ignore_units,
            case_insensitive=case_insensitive, engine=engine,
            snapshot_on_lock=snapshot_on_lock, snapshot_dir=snapshot_dir,
            path_pick=path_pick, path_default_glob=path_default_glob,
            path_loader=path_loader, loader_registry=loader_registry,
            enable_fuzzy=enable_fuzzy or bool(match_fuzzy),
            fuzzy_score_cutoff=fuzzy_score_cutoff,
            semantic_matcher=semantic_matcher
        )
        if get_all:
            df = reader.get_all(sheets=get_all_sheets, variables=get_all_vars,
                                include_all_columns=include_all_columns,
                                match_contains=match_contains, match_exact=match_exact,
                                match_regex=match_regex, match_fuzzy=match_fuzzy)
        elif query_sheet:
            df = reader.run_from_query_sheet(query_sheet, match_contains=match_contains,
                                             match_exact=match_exact, match_regex=match_regex,
                                             match_fuzzy=match_fuzzy)
        elif queries:
            norm = []
            for q in queries:
                sheet = q.get("sheet")
                ids = q.get("id_values") or q.get("ids") or []
                vars_ = q.get("vars") or q.get("variables") or []
                if isinstance(ids, str): ids = [s.strip() for s in ids.split(";") if s.strip()]
                if isinstance(vars_, str): vars_ = [s.strip() for s in vars_.split(";") if s.strip()]
                norm.append((sheet, ids, vars_))
            df = reader.get_many(norm, match_contains=match_contains, match_exact=match_exact,
                                 match_regex=match_regex, match_fuzzy=match_fuzzy)
        else:
            raise ValueError("Nothing to do: provide 'queries', or 'query_sheet', or set get_all=True.")

        if write_csv: reader.to_csv(df, write_csv)
        if write_parquet: reader.to_parquet(df, write_parquet)
        return df if return_df else None


def run_config(config: Union[str, Path, Dict]) -> pd.DataFrame:
    if isinstance(config, (str, Path)):
        cfg = json.loads(Path(config).read_text(encoding="utf-8"))
    else:
        cfg = dict(config)

    path = cfg["path"]
    opts = cfg.get("options", {}) or {}
    plan = cfg.get("plan", None)
    queries = cfg.get("queries", None)
    query_sheet = cfg.get("query_sheet", None)

    get_all_block = cfg.get("get_all", None)
    if get_all_block:
        get_all = True
        get_all_sheets = get_all_block.get("sheets", "*")
        get_all_vars = get_all_block.get("vars", None)
        include_all_columns = bool(get_all_block.get("include_all_columns", False))
    else:
        get_all = False
        get_all_sheets = "*"
        get_all_vars = None
        include_all_columns = False

    out = cfg.get("output", {}) or {}
    csv_path = out.get("csv")
    pq_path  = out.get("parquet") or out.get("pq") or None

    df = extract(
        path=path, plan=plan, queries=queries,
        default_id_candidates=tuple(opts.get("default_id_candidates", ("Identifier","ID","Sample","Name"))),
        ignore_units=bool(opts.get("ignore_units", True)),
        case_insensitive=bool(opts.get("case_insensitive", True)),
        engine=opts.get("engine", "openpyxl"),
        snapshot_on_lock=bool(opts.get("snapshot_on_lock", True)),
        snapshot_dir=opts.get("snapshot_dir"),
        query_sheet=query_sheet,
        get_all=get_all, get_all_sheets=get_all_sheets, get_all_vars=get_all_vars,
        include_all_columns=include_all_columns,
        match_contains=bool(opts.get("match_contains", True)),
        match_exact=bool(opts.get("match_exact", False)),
        match_regex=opts.get("match_regex", None),
        match_fuzzy=bool(opts.get("match_fuzzy", False)),
        write_csv=csv_path, write_parquet=pq_path,
        path_pick=opts.get("path_pick", "exact"),
        path_default_glob=opts.get("path_default_glob"),
        enable_fuzzy=bool(opts.get("enable_fuzzy", False)),
        fuzzy_score_cutoff=int(opts.get("fuzzy_score_cutoff", 82))
    )
    return df

def cli():
    import argparse
    parser = argparse.ArgumentParser(description="Run MetaReader from a JSON config.")
    parser.add_argument("--config", "-c", required=True, help="Path to JSON config")
    args = parser.parse_args()
    df = run_config(args.config)
    with pd.option_context("display.max_rows", 20, "display.width", 160):
        print(df.head(20).to_string(index=False))
