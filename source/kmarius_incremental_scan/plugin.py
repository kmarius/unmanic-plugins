import os
from typing import override

from unmanic.libs.unmodels import Libraries
from unmanic.libs.unplugins.settings import PluginSettings

from kmarius_incremental_scan.lib.plugin_types import *
from kmarius_incremental_scan.lib import timestamps, PLUGIN_ID, logger
from kmarius_incremental_scan.lib.panel import Panel


class Settings(PluginSettings):

    def __init__(self, *args, **kwargs):
        super(Settings, self).__init__(*args, **kwargs)
        self.settings, self.form_settings, self.configured_for = self.__build_settings()
        self._valid_extensions = None
        self._ignore_patterns = None
        # we trick unmanic into using only the global configuration
        self._library_id = self.library_id
        self.library_id = None

    @staticmethod
    def __build_settings():
        libraries = []
        for lib in Libraries().select().where(Libraries.enable_remote_only == False):
            libraries.append((lib.id, lib.name))

        settings = {
            "library_id": libraries[0][0],
        }

        form_settings = {
            "library_id": {
                "label": "Select library to configure - These settings only effects the data panel",
                "input_type": "select",
                "select_options": [
                    {"value": library_id, "label": name} for library_id, name in libraries
                ],
            },
        }

        settings.update({
            f"library_{library_id}_extensions": "" for library_id, _ in libraries
        })
        form_settings.update({
            f"library_{library_id}_extensions":
                {
                    "label": "Allowed extensions for this library",
                    "description": "Leave empty to show all contents. This setting only effects the data panel.",
                    "sub_setting": True,
                    "display": "hidden",
                }
            for library_id, _ in libraries
        })

        settings.update({
            f"library_{library_id}_ignored_paths": "" for library_id, _ in libraries
        })
        form_settings.update({
            f"library_{library_id}_ignored_paths":
                {
                    "label": "Ignored path patterns for this library - one per line",
                    "description": "This setting only effects the data panel.",
                    "input_type": "textarea",
                    "sub_setting": True,
                    "display": "hidden",
                }
            for library_id, _ in libraries
        })

        settings.update({
            f"library_{library_id}_hide_empty": True for library_id, _ in libraries
        })
        form_settings.update({
            f"library_{library_id}_hide_empty":
                {
                    "label": "Hide empty directories",
                    "description": "Hide directories e.g. if all its contents are filtered. This setting only effects the data panel.",
                    "sub_setting": True,
                    "display": "hidden",
                }
            for library_id, _ in libraries
        })

        settings.update({
            f"library_{library_id}_prune_ignored": False for library_id, _ in libraries
        })
        form_settings.update({
            f"library_{library_id}_prune_ignored":
                {
                    "label": "Prune directories early when loading libraries",
                    "description": "Prune directories early using the ignored patterns setting. This makes sense if your ignore patterns are meant to match directories. This setting only effects the data panel.",
                    "sub_setting": True,
                    "display": "hidden",
                }
            for library_id, _ in libraries
        })

        settings.update({
            f"library_{library_id}_lazy_load": False for library_id, _ in libraries
        })
        form_settings.update({
            f"library_{library_id}_lazy_load":
                {
                    "label": "Lazily load files in this library.",
                    "description": "Load contents of directories only when manually expanded. Can conflict with \"Hide empty directories\". This setting only effects the data panel.",
                    "sub_setting": True,
                    "display": "hidden",
                }
            for library_id, _ in libraries
        })

        settings.update({
            "quiet_incremental_scan": False,
        })
        form_settings.update({
            "quiet_incremental_scan": {
                "label": "Reduce logging",
                "description": "Don't log unchanged files and timestamp updates",
            },
        })

        library_ids = [lib[0] for lib in libraries]
        return settings, form_settings, library_ids

    @override
    def get_form_settings(self):
        form_settings = super(Settings, self).get_form_settings()
        if not self.settings_configured:
            # FIXME: in staging, settings_configured is not populated at this point and the corresponding method is private
            self._PluginSettings__import_configured_settings()
        if self.settings_configured:
            is_library_config = self._library_id is not None and self._library_id != 0
            if is_library_config:
                library_id = self._library_id
                form_settings["library_id"]["display"] = "hidden"
            else:
                library_id = self.settings_configured.get("library_id")
            library_settings = [
                f"library_{library_id}_extensions",
                f"library_{library_id}_ignored_paths",
                f"library_{library_id}_hide_empty",
                f"library_{library_id}_prune_ignored",
                f"library_{library_id}_lazy_load",
            ]
            for setting in library_settings:
                if setting in form_settings:
                    if is_library_config:
                        del form_settings[setting]["sub_setting"]
                    del form_settings[setting]["display"]
        return form_settings

    @override
    def reset_settings_to_defaults(self):
        library_id_str = f"_{self._library_id}_"

        defaults, _, _ = self.__build_settings()
        for setting, value in defaults.items():
            if library_id_str in setting:
                self.set_setting(setting, value)

        return True


timestamps.init()
panel = Panel(Settings)
settings = Settings()


def is_file_unchanged(library_id: int, path: str) -> bool:
    mtime = int(os.path.getmtime(path))
    stored_timestamp = timestamps.get(library_id, path, reuse_connection=True)
    return stored_timestamp == mtime


def update_timestamp(library_id: int, path: str) -> int | None:
    try:
        mtime = int(os.path.getmtime(path))
        timestamps.put(library_id, path, mtime)
        return mtime
    except Exception as e:
        logger.error(e)


def on_library_management_file_test(data: FileTestData):
    library_id = data.get('library_id')
    path = data.get("path")

    quiet = settings.get_setting("quiet_incremental_scan")

    if is_file_unchanged(library_id, path):
        if not quiet:
            data["issues"].append({
                'id': PLUGIN_ID,
                'message': f"unchanged: library_id={library_id} path={path}",
            })
        data['add_file_to_pending_tasks'] = False
    else:
        data["shared_info"]["quiet_incremental_scan"] = quiet


def on_postprocessor_task_results(data: TaskResultData):
    # we are assuming here that all output files belong to the same library
    # and that we don't want to test it again in the future

    quiet = settings.get_setting("quiet_incremental_scan")

    if data["task_processing_success"] and data["file_move_processes_success"]:
        library_id = data["library_id"]
        for path in data["destination_files"]:
            try:
                mtime = update_timestamp(library_id, path)
                if mtime and not quiet:
                    logger.info(f"Updated timestamp library_id={library_id} path={path} to {mtime}")
            except Exception as e:
                logger.error(e)


def render_frontend_panel(data: PanelData):
    panel.render_frontend_panel(data)


def render_plugin_api(data: PluginApiData):
    panel.render_plugin_api(data)