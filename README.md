# metareader

**metareader** lets you pull values by **Identifier & Variable** from large Excel/CSV/Parquet datasets — fast.

- Constant-time ID lookups (per-sheet index)
- Reads only requested columns; caches until file changes
- Snapshot-on-lock (works even if Excel has the workbook open)
- Multi-file merge via `MultiMetaReader`
- Matching modes: contains / exact / regex / (optional) fuzzy
- Output helpers to CSV / Parquet

## Install (editable dev mode)

```bash
pip install -e .
# optional extras
pip install "metareader[fuzzy]"     # RapidFuzz
pip install "metareader[parquet]"   # PyArrow
