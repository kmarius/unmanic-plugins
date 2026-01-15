#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os

from unmanic.libs.unplugins.settings import *
from kmarius_incremental_scan_db.lib import store_timestamp

logger = logging.getLogger("Unmanic.Plugin.kmarius_incremental_scan_db")


class Settings(PluginSettings):
    settings = {}

    def __init__(self, *args, **kwargs):
        super(Settings, self).__init__(*args, **kwargs)


def update_timestamp(library_id: int, path: str):
    file_stat = os.stat(path)
    timestamp = int(file_stat.st_mtime)
    logger.info(f"Updating timestamp path={path} library_id={
                library_id} to {timestamp}")
    store_timestamp(library_id, path, timestamp)


def on_library_management_file_test(data: dict):
    # if this tester is reached, the file passed all checks
    # - update the stored timestamp

    update_timestamp(data["library_id"], data["path"])

    return data


def on_postprocessor_task_results(data: dict):
    # we are assuming here that all output files belong to the same library
    # and that we don't want to test it again in the future
    if data["task_processing_success"] and data["file_move_processes_success"]:
        library_id = data["library_id"]
        for path in data["destination_files"]:
            try:
                update_timestamp(library_id, path)
            except Exception as e:
                logger.error(e)
    return data