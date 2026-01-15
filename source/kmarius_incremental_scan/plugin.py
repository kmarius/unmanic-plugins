#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os

from unmanic.libs.unplugins.settings import PluginSettings
from kmarius_incremental_scan_db.lib import load_timestamp, store_timestamp

logger = logging.getLogger("Unmanic.Plugin.kmarius_incremental_scan")


class Settings(PluginSettings):
    settings = {
        "ignore_timestamps": False
    }
    form_settings = {
        "ignore_timestamps": {
            "label": "Disable timestamp checking.",
        },
    }

    def __init__(self, *args, **kwargs):
        super(Settings, self).__init__(*args, **kwargs)


def on_library_management_file_test(data):
    settings = Settings(library_id=data.get('library_id'))
    if settings.get_setting('ignore_timestamps'):
        return data

    library_id = data.get('library_id')
    path = data.get("path")
    file_stat = os.stat(path)
    disk_timestamp = int(file_stat.st_mtime)
    stored_timestamp = load_timestamp(library_id, path)

    if stored_timestamp == disk_timestamp:
        data['add_file_to_pending_tasks'] = False
        data["issues"].append({
            'id': "kmarius_incremental_scan",
            'message': f"file unchanged: {path}, library_id={library_id}"
        })

    return data
