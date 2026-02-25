import logging
from typing import Dict

PLUGIN_ID = "kmarius_incremental_scan"

logger = logging.getLogger(f"Unmanic.Plugin.{PLUGIN_ID}")

# this dict holds all files with their current timestamp (per-library) that were sent down the file-test pipeline
# we remove them, once a file is added to the pending queue. Of all files that remain
# we update the timestamp once the scan completes.
_files_tested: Dict[int, Dict[str, int]] = {}


def add_file_tested(library_id: int, path: str, mtime: int):
    if library_id not in _files_tested:
        _files_tested[library_id] = {}
    _files_tested[library_id][path] = mtime


def remove_file_tested(library_id: int, path: str):
    if library_id not in _files_tested:
        _files_tested[library_id] = {}
    if path in _files_tested[library_id]:
        del _files_tested[library_id]


def get_files_tested(library_id: int, clear=True) -> Dict[str, int]:
    if library_id not in _files_tested:
        return {}
    res = _files_tested[library_id]
    if clear:
        del _files_tested[library_id]
    return res