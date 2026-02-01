import json
import os
import queue
import threading
import time
import traceback
import uuid
from typing import Mapping, Optional, Set

from unmanic.libs.filetest import FileTesterThread
from unmanic.libs.libraryscanner import LibraryScannerManager
from unmanic.libs.unmodels import Libraries

from .plugin_types import *
from . import timestamps, logger


def critical(f):
    """Decorator to allow only one thread to execute this function at a time."""
    lock = threading.Lock()

    def wrapped(*args, **kwargs):
        if not lock.acquire(blocking=False):
            logger.info("Could not acquire lock")
            return
        try:
            f(*args, **kwargs)
        finally:
            lock.release()

    return wrapped


def _get_thread(name: str) -> Optional[threading.Thread]:
    for thread in threading.enumerate():
        if thread.name == name:
            return thread
    return None


def _get_libraryscanner() -> LibraryScannerManager:
    return _get_thread("LibraryScannerManager")


def _get_library_paths() -> Mapping[int, str]:
    paths = {}
    for lib in Libraries().select().where(Libraries.enable_remote_only == False):
        paths[lib.id] = lib.path
    return paths


def _get_libraries(lazy=True) -> dict:
    libraries = []
    for lib in Libraries().select().where(Libraries.enable_remote_only == False):
        libraries.append({
            "title":      lib.name,
            "library_id": lib.id,
            "path":       lib.path,
            "type":       "folder",
            "lazy":       lazy,
        })

    return {
        "children": libraries,
    }


def _validate_path(path: str, library_path: str) -> bool:
    return path.startswith("/") and "/.." not in path and path.startswith(library_path)


def _expand_path(path: str) -> list[str]:
    res = []
    for dirpath, _, filenames in os.walk(path):
        for filename in filenames:
            res.append(os.path.join(dirpath, filename))
    return res


# possibly make this configurable
def _get_icon(name: str) -> str:
    ext = os.path.splitext(name)[1][1:].lower()
    if ext in ["mp4", "mkv", "webm", "avi", "mov", "flv"]:
        return "bi bi-film"
    elif ext in ["mp3", "m4a", "flac", "opus", "ogg"]:
        return "bi bi-music-note-beamed"
    elif ext in ["jpg", "png", "bmp"]:
        return "bi bi-image"
    else:
        return "bi bi-file-earmark"


def _test_files_in_lib(library_id: int, items: Set[str]):
    num_files = len(items)
    if num_files == 0:
        return

    libraryscanner = _get_libraryscanner()
    num_threads = libraryscanner.settings.get_concurrent_file_testers()

    # pre-fill queue
    files_to_test = queue.Queue()
    for item in items:
        files_to_test.put(item)
    files_to_process = queue.Queue()

    event = libraryscanner.event
    status_updates = queue.Queue()
    frontend_messages = libraryscanner.data_queues.get('frontend_messages')

    def send_frontend_message(message):
        frontend_messages.update(
            {
                'id':      'libraryScanProgress',
                'type':    'status',
                'code':    'libraryScanProgress',
                'message': message,
                'timeout': 0
            }
        )

    threads = []

    for i in range(num_threads):
        tester = FileTesterThread(f"kmarius-file-tester-{library_id}-{i}",
                                  files_to_test, files_to_process, status_updates,
                                  library_id, event)
        tester.daemon = True
        tester.start()
        threads.append(tester)

    def queue_up_result(item):
        libraryscanner.add_path_to_queue(
            item.get('path'), library_id, item.get('priority_score'))

    current_file = ''
    while not files_to_test.empty():
        while not files_to_process.empty():
            queue_up_result(files_to_process.get())

        if not status_updates.empty():
            current_file = status_updates.get()
            while not status_updates.empty():
                current_file = status_updates.get()
            percent_completed = (num_files - files_to_test.qsize()) / num_files * 100
            percent_completed_string = '{:.0f}% - Testing: {}'.format(percent_completed, current_file)
            send_frontend_message(percent_completed_string)

        event.wait(0.1)

    while not status_updates.empty():
        current_file = status_updates.get()
    percent_completed_string = '{:.0f}% - Testing: {}'.format(100, current_file)
    send_frontend_message(percent_completed_string)

    for thread in threads:
        thread.stop()

    for thread in threads:
        thread.join()

    while not files_to_process.empty():
        queue_up_result(files_to_process.get())

    frontend_messages.remove_item('libraryScanProgress')


@critical
def _test_files_thread(items_per_lib: Mapping[int, Set[str]]):
    for library_id, paths in items_per_lib.items():
        _test_files_in_lib(library_id, paths)


class Panel:
    # we pass the settings class and create the instances for different libraries as needed
    def __init__(self, settings_class):
        self.Settings = settings_class
        # we can cache some things meaningfully. allowed extensions for example, because when they change plugin.py is
        # re-executed and the Panel is re-created
        self._allowed_extensions = {}

    def _get_allowed_extensions(self, library_id: int) -> list[str]:
        if library_id not in self._allowed_extensions:
            settings = self.Settings(library_id=library_id)
            extensions = settings.get_setting("allowed_extensions").split(",")
            extensions = [ext.strip().lstrip(".") for ext in extensions]
            self._allowed_extensions[library_id] = extensions
        return self._allowed_extensions[library_id]

    def _is_extension_allowed(self, library_id: int, path: str) -> bool:
        extensions = self._get_allowed_extensions(library_id)
        _, ext = os.path.splitext(path)
        return ext.lstrip(".").lower() in extensions

    def _test_files(self, payload: dict):
        library_paths = _get_library_paths()

        if "arr" in payload:
            items = payload["arr"]
        else:
            items = [payload]

        items_per_lib = {}

        for item in items:
            library_id = item["library_id"]
            path = item["path"]

            if not _validate_path(path, library_paths[library_id]):
                raise Exception("Invalid path")

            if not library_id in items_per_lib:
                items_per_lib[library_id] = set()

            if os.path.isdir(path):
                items_ = items_per_lib[library_id]
                for path in _expand_path(path):
                    if self._is_extension_allowed(library_id, path):
                        items_.add(path)
            else:
                items_per_lib[library_id].add(path)

        threading.Thread(
            target=_test_files_thread,
            args=(items_per_lib,),
            daemon=True
        ).start()

    def _process_files(self, payload: dict):
        library_paths = _get_library_paths()

        libraryscanner = _get_libraryscanner()

        if "arr" in payload:
            items = payload["arr"]
        else:
            items = [payload]

        items_per_lib = {}

        for item in items:
            library_id = item["library_id"]
            path = item["path"]
            priority_score = item["priority_score"]

            if not _validate_path(path, library_paths[library_id]):
                raise Exception("Invalid path")

            if not library_id in items_per_lib:
                items_per_lib[library_id] = []

            if os.path.isdir(path):
                items_ = items_per_lib[library_id]
                for path in _expand_path(path):
                    if self._is_extension_allowed(library_id, path):
                        items_.append(
                            {"path": path, "priority_score": priority_score})
            else:
                items_per_lib[library_id].append(
                    {"path": path, "priority_score": priority_score})

        for library_id, items in items_per_lib.items():
            for item in items:
                libraryscanner.add_path_to_queue(
                    item['path'], library_id, item['priority_score'])

    # this function can't load single files currently, only directories with their files
    def _load_subtree(self, path: str, title: str, library_id: int, lazy=True, get_timestamps=False) -> dict:
        children = []
        files = []

        with os.scandir(path) as entries:
            for entry in entries:
                name = entry.name
                if name.startswith("."):
                    continue
                abspath = os.path.abspath(os.path.join(path, name))
                if entry.is_dir():
                    if lazy:
                        children.append({
                            "title":      name,
                            "library_id": library_id,
                            "path":       abspath,
                            "lazy":       True,
                            "type":       "folder",
                        })
                    else:
                        children.append(self._load_subtree(
                            abspath, name, library_id, lazy=False, get_timestamps=get_timestamps))
                else:
                    if self._is_extension_allowed(library_id, name):
                        file_info = os.stat(abspath)
                        files.append({
                            "title":      name,
                            "library_id": library_id,
                            "path":       abspath,
                            "mtime":      int(file_info.st_mtime),
                            "size":       int(file_info.st_size),
                            "icon":       _get_icon(name),
                        })

        children.sort(key=lambda c: c["title"])
        files.sort(key=lambda c: c["title"])

        # getting timestamps in bulk makes the operation >5 times faster
        if get_timestamps:
            paths = [file["path"] for file in files]
            for i, timestamp in enumerate(timestamps.get_many(library_id, paths)):
                files[i]['timestamp'] = timestamp

        children += files

        return {
            "title":      title,
            "children":   children,
            "library_id": library_id,
            "path":       path,
            "type":       "folder",
        }

    def _get_subtree(self, arguments: dict, lazy=True) -> dict:
        library_id = int(arguments["library_id"][0])
        path = arguments["path"][0].decode('utf-8')
        title = arguments["title"][0].decode('utf-8')

        library = Libraries().select().where(Libraries.id == library_id).first()

        if library.enable_remote_only:
            raise Exception("Library is remote only")

        if not path.startswith(library.path) or "/.." in path:
            raise Exception("Invalid path")

        return self._load_subtree(path, title, library_id, lazy=lazy, get_timestamps=True)

    def _reset_timestamps(self, payload: dict):
        if "arr" in payload:
            items = [(item["library_id"], item["path"]) for item in payload["arr"]]
        else:
            items = [(payload["library_id"], payload["path"])]

        distinct = set()
        for library_id, path in items:
            if os.path.isdir(path):
                for p in _expand_path(path):
                    distinct.add((library_id, p))
            else:
                distinct.add((library_id, path))
        values = [(library_id, path, 0) for library_id, path in distinct if
                  self._is_extension_allowed(library_id, path)]

        timestamps.put_many(values)

    def _update_timestamps(self, payload: dict):
        if "arr" in payload:
            items = [(item["library_id"], item["path"]) for item in payload["arr"]]
        else:
            items = [(payload["library_id"], payload["path"])]

        distinct = set()
        for library_id, path in items:
            if os.path.isdir(path):
                for p in _expand_path(path):
                    distinct.add((library_id, p))
            else:
                distinct.add((library_id, path))
        items = [(library_id, path) for library_id,
        path in distinct if self._is_extension_allowed(library_id, path)]

        values = []
        for library_id, path in items:
            try:
                mtime = int(os.path.getmtime(path))
                values.append((library_id, path, mtime))
            except OSError as e:
                logger.error(f"{e}")

        timestamps.put_many(values)

    @critical
    def _prune_database(self, payload: dict):
        library_ids = []

        if "library_id" in payload:
            library_ids.append(payload["library_id"])
        else:
            for lib in Libraries().select().where(Libraries.enable_remote_only == False):
                library_ids.append(lib.id)

        num_pruned = 0
        for library_id in library_ids:
            logger.info(f"Pruning library {library_id}")

            paths = []
            for path in timestamps.get_all_paths(library_id):
                if not self._is_extension_allowed(library_id, path) or not os.path.exists(path):
                    paths.append(path)

            timestamps.remove_paths(library_id, paths)

            num_pruned += len(paths)
        logger.info(f"Pruned {num_pruned} orphans")
        time.sleep(1)

    @staticmethod
    def render_frontend_panel(data: PanelData):
        data["content_type"] = "text/html"

        # TODO: change PLUGIN_ID in the served file so we can re-use index.html
        with open(os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'static', 'index.html'))) as file:
            content = file.read()
            data['content'] = content.replace("{cache_buster}", str(uuid.uuid4()))

    def render_plugin_api(self, data: PluginApiData):
        data['content_type'] = 'application/json'

        path = data["path"]

        try:
            if path == "/test":
                self._test_files(json.loads(data["body"].decode('utf-8')))
            elif path == '/process':
                self._process_files(json.loads(data["body"].decode('utf-8')))
            elif path == '/subtree':
                data["content"] = self._get_subtree(data["arguments"], False)
            elif path == "/libraries":
                data["content"] = _get_libraries()
            elif path == "/timestamp/reset":
                self._reset_timestamps(json.loads(data["body"].decode('utf-8')))
            elif path == "/timestamp/update":
                self._update_timestamps(json.loads(data["body"].decode('utf-8')))
            elif path == "/prune":
                body = data["body"].decode('utf-8')
                if body.startswith("{"):
                    payload = json.loads(body)
                else:
                    payload = {}
                threading.Thread(target=self._prune_database,
                                 args=(payload,),
                                 daemon=True).start()
            else:
                data["content"] = {
                    "success": False,
                    "error":   f"unknown path: {data['path']}",
                }
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(trace)
            data["content"] = {
                "success": False,
                "error":   str(e),
                "trace":   trace,
            }