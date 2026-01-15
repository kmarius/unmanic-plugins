#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os

from unmanic.libs.unplugins.settings import PluginSettings
from kmarius_incremental_scan_db.lib import load_timestamp, store_timestamp

logger = logging.getLogger("Unmanic.Plugin.kmarius_incremental_scan_db")


class Settings(PluginSettings):
    settings = {}

    def __init__(self, *args, **kwargs):
        super(Settings, self).__init__(*args, **kwargs)


def on_library_management_file_test(data):
    # if this tester is reached, the file passed all checks - update the stored timestamp

    library_id = data.get("library_id")
    path = data.get("path")
    file_stat = os.stat(path)
    timestamp = int(file_stat.st_mtime)
    logger.info(f"Updating timestamp path={path} library_id={library_id} to {timestamp}")
    store_timestamp(library_id, path, timestamp)

    return data


def on_postprocessor_task_results(data):
    if data["task_processing_success"] and data["file_move_processes_success"]:
        library_id = data.get("library_id")
        for path in data["destination_files"]:
            try:
                file_stat = os.stat(path)
                timestamp = int(file_stat.st_mtime)
                logger.info(f"Updating timestamp path={path} library_id={library_id} to {timestamp}")
                store_timestamp(library_id, path, timestamp)
            except Exception as e:
                logger.error(e)
    return data
