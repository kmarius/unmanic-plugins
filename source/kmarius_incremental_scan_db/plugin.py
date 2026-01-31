#!/usr/bin/env python3

import logging
import os

from kmarius_incremental_scan_db.lib.plugin_types import *

logger = logging.getLogger("Unmanic.Plugin.kmarius_incremental_scan_db")


def update_timestamp(library_id: int, path: str):
    from kmarius_incremental_scan.lib import timestamps
    try:
        mtime = int(os.path.getmtime(path))
        logger.info(f"Updating timestamp library_id={library_id} path={path} to {mtime}")
        timestamps.put(library_id, path, mtime)
    except Exception as e:
        logger.error(e)


def on_library_management_file_test(data: dict):
    update_timestamp(data["library_id"], data["path"])
    return data