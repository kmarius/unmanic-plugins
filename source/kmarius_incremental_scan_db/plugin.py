import logging
import os

from kmarius_incremental_scan_db.lib.plugin_types import *

logger = logging.getLogger("Unmanic.Plugin.kmarius_incremental_scan_db")


def update_timestamp(library_id: int, path: str) -> int | None:
    from kmarius_incremental_scan.lib import timestamps

    try:
        mtime = int(os.path.getmtime(path))
        timestamps.put(library_id, path, mtime)
        return mtime
    except Exception as e:
        logger.error(e)


def on_library_management_file_test(data: FileTestData):
    quiet = data["shared_info"].get("quiet_incremental_scan", False)
    library_id = data["library_id"]
    path = data["path"]
    mtime = update_timestamp(library_id, path)
    if mtime and not quiet:
        logger.info(f"Updated timestamp library_id={library_id} path={path} to {mtime}")
    return data