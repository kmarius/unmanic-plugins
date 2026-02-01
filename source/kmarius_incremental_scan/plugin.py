#!/usr/bin/env python3

import os

from unmanic.libs.unplugins.settings import PluginSettings
from kmarius_incremental_scan.lib.plugin_types import *
from kmarius_incremental_scan.lib import timestamps, PLUGIN_ID, logger
from kmarius_incremental_scan.lib.panel import Panel


class Settings(PluginSettings):
    settings = {
        "allowed_extensions": "mp4,mkv,webm,avi,mov,flv,mp3,m4a,flac",
    }
    form_settings = {
        "allowed_extensions": {
            "label":       "Allowed extensions, separated by comma",
            "description": "This setting is only used to limit what is shown in the data panel.",
        }
    }

    def __init__(self, *args, **kwargs):
        super(Settings, self).__init__(*args, **kwargs)


timestamps.init()
panel = Panel(Settings)


def is_file_unchanged(library_id: int, path: str) -> bool:
    mtime = int(os.path.getmtime(path))
    stored_timestamp = timestamps.get(library_id, path, reuse_connection=True)
    return stored_timestamp == mtime


def update_timestamp(library_id: int, path: str):
    try:
        mtime = int(os.path.getmtime(path))
        timestamps.put(library_id, path, mtime)
    except Exception as e:
        logger.error(e)


def on_library_management_file_test(data: FileTestData):
    library_id = data.get('library_id')
    path = data.get("path")

    if is_file_unchanged(library_id, path):
        data["issues"].append({
            'id':      PLUGIN_ID,
            'message': f"unchanged: library_id={library_id} path={path}",
        })
        data['add_file_to_pending_tasks'] = False


def on_postprocessor_task_results(data: TaskResultData):
    # we are assuming here that all output files belong to the same library
    # and that we don't want to test it again in the future
    if data["task_processing_success"] and data["file_move_processes_success"]:
        library_id = data["library_id"]
        for path in data["destination_files"]:
            try:
                update_timestamp(library_id, path)
            except Exception as e:
                logger.error(e)


def render_frontend_panel(data: PanelData):
    panel.render_frontend_panel(data)


def render_plugin_api(data: PluginApiData):
    panel.render_plugin_api(data)