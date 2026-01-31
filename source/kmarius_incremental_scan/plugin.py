#!/usr/bin/env python3

import json
import os
import queue
import threading
import traceback
import uuid
from typing import Mapping, Optional

from unmanic.libs.filetest import FileTesterThread
from unmanic.libs.libraryscanner import LibraryScannerManager
from unmanic.libs.unmodels import Libraries
from unmanic.libs.unplugins.settings import PluginSettings
from kmarius_incremental_scan.lib.plugin_types import *
from kmarius_incremental_scan.lib import timestamps, PLUGIN_ID, logger

timestamps.init()


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


_allowed_extensions = {}
_ignored_path_patterns = {}


def get_allowed_extensions(library_id: int) -> list[str]:
    if library_id not in _allowed_extensions:
        settings = Settings(library_id=library_id)
        extensions = settings.get_setting("allowed_extensions").split(",")
        extensions = [ext.strip().lstrip(".") for ext in extensions]
        _allowed_extensions[library_id] = extensions
    return _allowed_extensions[library_id]


def is_extension_allowed(library_id: int, path: str) -> bool:
    extensions = get_allowed_extensions(library_id)
    ext = os.path.splitext(path)[-1]
    if ext and ext[1:].lower() in extensions:
        return True
    return False


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


def on_library_management_file_test(data: FileTestData) -> Optional[FileTestData]:
    library_id = data.get('library_id')
    path = data.get("path")

    if is_file_unchanged(library_id, path):
        data["issues"].append({
            'id':      PLUGIN_ID,
            'message': f"unchanged: library_id={library_id} path={path}",
        })
        data['add_file_to_pending_tasks'] = False

    return data


def on_postprocessor_task_results(data: TaskResultData) -> Optional[TaskResultData]:
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


def get_thread(name: str) -> Optional[threading.Thread]:
    for thread in threading.enumerate():
        if thread.name == name:
            return thread
    return None


def get_libraryscanner() -> LibraryScannerManager:
    return get_thread("LibraryScannerManager")


def expand_path(path: str) -> list[str]:
    res = []
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            res.append(os.path.join(dirpath, filename))
    return res


def get_library_paths() -> Mapping[int, str]:
    paths = {}
    for lib in Libraries().select().where(Libraries.enable_remote_only == False):
        paths[lib.id] = lib.path
    return paths


def validate_path(path: str, library_path: str) -> bool:
    return ".." not in path and path.startswith(library_path)


def test_file_thread(items: list, library_id: int, num_threads=1):
    if len(items) == 0:
        return

    libraryscanner = get_libraryscanner()

    # pre-fill queue
    files_to_test = queue.Queue()
    for item in items:
        files_to_test.put(item)
    files_to_process = queue.Queue()

    event = libraryscanner.event

    threads = []

    for i in range(num_threads):
        tester = FileTesterThread(f"kmarius-file-tester-{library_id}-{i}",
                                  files_to_test, files_to_process, queue.Queue(),
                                  library_id, event)
        tester.daemon = True
        tester.start()
        threads.append(tester)

    def queue_up_result(item):
        libraryscanner.add_path_to_queue(
            item.get('path'), library_id, item.get('priority_score'))

    while not files_to_test.empty():
        while not files_to_process.empty():
            queue_up_result(files_to_process.get())
        event.wait(1)

    for thread in threads:
        thread.stop()

    for thread in threads:
        thread.join()

    while not files_to_process.empty():
        queue_up_result(files_to_process.get())


def test_files(payload: dict):
    library_paths = get_library_paths()

    if "arr" in payload:
        items = payload["arr"]
    else:
        items = [payload]

    items_per_lib = {}

    for item in items:
        library_id = item["library_id"]
        path = item["path"]

        if not validate_path(path, library_paths[library_id]):
            raise Exception("Invalid path")

        if not library_id in items_per_lib:
            items_per_lib[library_id] = set()

        if os.path.isdir(path):
            items_ = items_per_lib[library_id]
            for path in expand_path(path):
                if is_extension_allowed(library_id, path):
                    items_.add(path)
        else:
            items_per_lib[library_id].add(path)

    for library_id, items in items_per_lib.items():
        threading.Thread(target=test_file_thread, args=(
            list(items), library_id), daemon=True).start()


def process_files(payload: dict):
    library_paths = get_library_paths()

    libraryscanner = get_libraryscanner()

    if "arr" in payload:
        items = payload["arr"]
    else:
        items = [payload]

    items_per_lib = {}

    for item in items:
        library_id = item["library_id"]
        path = item["path"]
        priority_score = item["priority_score"]

        if not validate_path(path, library_paths[library_id]):
            raise Exception("Invalid path")

        if not library_id in items_per_lib:
            items_per_lib[library_id] = []

        if os.path.isdir(path):
            items_ = items_per_lib[library_id]
            for path in expand_path(path):
                if is_extension_allowed(library_id, path):
                    items_.append(
                        {"path": path, "priority_score": priority_score})
        else:
            items_per_lib[library_id].append(
                {"path": path, "priority_score": priority_score})

    for library_id, items in items_per_lib.items():
        for item in items:
            libraryscanner.add_path_to_queue(
                item['path'], library_id, item['priority_score'])


# possibly make this configurable
def get_icon(name: str) -> str:
    ext = os.path.splitext(name)[1][1:].lower()
    if ext in ["mp4", "mkv", "webm", "avi", "mov", "flv"]:
        return "bi bi-film"
    elif ext in ["mp3", "m4a", "flac", "opus", "ogg"]:
        return "bi bi-music-note-beamed"
    elif ext in ["jpg", "png", "bmp"]:
        return "bi bi-image"
    else:
        return "bi bi-file-earmark"


# this function can't load single files currently, only directories with their files
def load_subtree(path: str, title: str, library_id: int, lazy=True, get_timestamps=False) -> dict:
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
                    children.append(load_subtree(
                        abspath, name, library_id, lazy=False, get_timestamps=get_timestamps))
            else:
                if is_extension_allowed(library_id, name):
                    file_info = os.stat(abspath)
                    files.append({
                        "title":      name,
                        "library_id": library_id,
                        "path":       abspath,
                        "mtime":      int(file_info.st_mtime),
                        "size":       int(file_info.st_size),
                        "icon":       get_icon(name),
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


def get_subtree(arguments: dict, lazy=True) -> dict:
    library_id = int(arguments["library_id"][0])
    path = arguments["path"][0].decode('utf-8')
    title = arguments["title"][0].decode('utf-8')

    library = Libraries().select().where(Libraries.id == library_id).first()

    if library.enable_remote_only:
        raise Exception("Library is remote only")

    if not path.startswith(library.path) or ".." in path:
        raise Exception("Invalid path")

    return load_subtree(path, title, library_id, lazy=lazy, get_timestamps=True)


def reset_timestamps(payload: dict):
    if "arr" in payload:
        items = [(item["library_id"], item["path"]) for item in payload["arr"]]
    else:
        items = [(payload["library_id"], payload["path"])]

    distinct = set()
    for library_id, path in items:
        if os.path.isdir(path):
            for p in expand_path(path):
                distinct.add((library_id, p))
        else:
            distinct.add((library_id, path))
    values = [(library_id, path, 0) for library_id, path in distinct if
              is_extension_allowed(library_id, path)]

    timestamps.put_many(values)


def update_timestamps(payload: dict):
    if "arr" in payload:
        items = [(item["library_id"], item["path"]) for item in payload["arr"]]
    else:
        items = [(payload["library_id"], payload["path"])]

    distinct = set()
    for library_id, path in items:
        if os.path.isdir(path):
            for p in expand_path(path):
                distinct.add((library_id, p))
        else:
            distinct.add((library_id, path))
    items = [(library_id, path) for library_id,
    path in distinct if is_extension_allowed(library_id, path)]

    values = []
    for library_id, path in items:
        try:
            mtime = int(os.path.getmtime(path))
            values.append((library_id, path, mtime))
        except OSError as e:
            logger.error(f"{e}")

    timestamps.put_many(values)


def get_libraries(lazy=True) -> dict:
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


@critical
def prune_database(payload: dict):
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
            if not is_extension_allowed(library_id, path) or not os.path.exists(path):
                paths.append(path)

        timestamps.remove_paths(library_id, paths)

        num_pruned += len(paths)
    logger.info(f"Pruned {num_pruned} orphans")


def render_frontend_panel(data: PanelData):
    data["content_type"] = "text/html"

    with open(os.path.abspath(os.path.join(os.path.dirname(__file__), 'static', 'index.html'))) as file:
        content = file.read()
        data['content'] = content.replace("{cache_buster}", str(uuid.uuid4()))


def render_plugin_api(data: PluginApiData) -> PluginApiData:
    data['content_type'] = 'application/json'

    path = data["path"]

    try:
        if path == "/test":
            test_files(json.loads(data["body"].decode('utf-8')))
        elif path == '/process':
            process_files(json.loads(data["body"].decode('utf-8')))
        elif path == '/subtree':
            data["content"] = get_subtree(data["arguments"], False)
        elif path == "/libraries":
            data["content"] = get_libraries()
        elif path == "/timestamp/reset":
            reset_timestamps(json.loads(data["body"].decode('utf-8')))
        elif path == "/timestamp/update":
            update_timestamps(json.loads(data["body"].decode('utf-8')))
        elif path == "/prune":
            body = data["body"].decode('utf-8')
            if body.startswith("{"):
                payload = json.loads(body)
            else:
                payload = {}
            threading.Thread(target=prune_database, args=(
                payload,), daemon=True).start()
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

    return data