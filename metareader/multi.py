from __future__ import annotations
from typing import Dict, List, Sequence, Union, Optional, Tuple, Any
from pathlib import Path
import pandas as pd
from .core import MetaReader, resolve_data_path, _as_list

__all__ = ["MultiMetaReader"]

class MultiMetaReader:
    """
    Wrapper that applies the same plan/queries across multiple Excel files and concatenates results.
    Each underlying MetaReader handles one resolved file. Results include a 'Source' column.
    """
    def __init__(self,
                 paths: Sequence[Union[str, Path, Dict[str, Any]]],
                 *,
                 plan: Optional[Dict[str, Union[str, Sequence[str]]]] = None,
                 default_id_candidates: Sequence[str] = ("Identifier", "ID", "Sample", "Name"),
                 ignore_units: bool = True,
                 case_insensitive: bool = True,
                 engine: str = "openpyxl",
                 snapshot_on_lock: bool = True,
                 snapshot_dir: Optional[Union[str, Path]] = None,
                 path_pick: str = "exact",
                 path_default_glob: Optional[str] = None,
                 path_loader=None,
                 loader_registry=None,
                 enable_fuzzy: bool = False,
                 fuzzy_score_cutoff: int = 82,
                 semantic_matcher=None):
        self.readers: List[MetaReader] = []
        for p in _as_list(paths):
            resolved = resolve_data_path(p, pick=path_pick, default_glob=path_default_glob,
                                         path_loader=path_loader, loader_registry=loader_registry)
            if resolved is None:
                continue
            self.readers.append(
                MetaReader(
                    path=str(resolved), plan=plan, default_id_candidates=default_id_candidates,
                    ignore_units=ignore_units, case_insensitive=case_insensitive, engine=engine,
                    snapshot_on_lock=snapshot_on_lock, snapshot_dir=snapshot_dir,
                    enable_fuzzy=enable_fuzzy, fuzzy_score_cutoff=fuzzy_score_cutoff,
                    semantic_matcher=semantic_matcher
                )
            )
        if not self.readers:
            raise FileNotFoundError("No valid files resolved for MultiMetaReader")

    def list_sheets(self) -> List[str]:
        s = set()
        for r in self.readers:
            s.update(r.list_sheets())
        return sorted(s)

    def get(self, sheet: str, id_values, variables, **kwargs) -> pd.DataFrame:
        frames = []
        for r in self.readers:
            if sheet not in r.list_sheets():
                continue
            df = r.get(sheet, id_values, variables, **kwargs)
            if not df.empty:
                df.insert(0, "Source", Path(r.path).name)
                frames.append(df)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def get_many(self, queries, **kwargs) -> pd.DataFrame:
        frames = []
        for r in self.readers:
            local = []
            for (sh, ids, vs) in queries:
                if sh in r.list_sheets():
                    local.append((sh, ids, vs))
            if local:
                df = r.get_many(local, **kwargs)
                if not df.empty:
                    df.insert(0, "Source", Path(r.path).name)
                    frames.append(df)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def get_all(self, **kwargs) -> pd.DataFrame:
        frames = []
        for r in self.readers:
            df = r.get_all(**kwargs)
            if not df.empty:
                df.insert(0, "Source", Path(r.path).name)
                frames.append(df)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
