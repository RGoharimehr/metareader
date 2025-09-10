# metareader/__init__.py

from .core import MetaReader  # and optionally: resolve_data_path, etc., if present
from .multi import MultiMetaReader
from .runner import extract, run_config

__all__ = ["MetaReader", "MultiMetaReader", "extract", "run_config"]

# Keep this in sync with pyproject.toml [project].version
__version__ = "0.3.0"
