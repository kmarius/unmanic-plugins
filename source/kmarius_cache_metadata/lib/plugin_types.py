from typing import TypedDict, Callable


class PanelData (TypedDict):
    content_type: str
    content: str
    path: str
    arguments: dict


class PluginApiData (TypedDict):
    content_type: str
    content: dict
    path: str
    uri: str
    query: str
    arguments: dict
    body: bytes


class FileTestData (TypedDict):
    library_id: int
    path: str
    issues: list
    add_file_to_pending_tasks: bool
    priority_score: int
    shared_info: dict


class FileMoveData (TypedDict):
    library_id: int
    source_data: dict
    remove_source_file: bool
    copy_file: bool
    file_in: str
    file_out: str
    run_default_file_copy: bool


class TaskResultData (TypedDict):
    final_cache_path: str
    library_id: int
    task_processing_success: bool
    file_move_processes_success: bool
    destination_files: list
    source_data: dict


class ProcessItemData (TypedDict):
    worker_log: list
    library_id: int
    exec_command: list[str]
    command_progress_parser: Callable[[str], dict]
    file_in: str
    file_out: str
    original_file_path: str
    repeat: bool