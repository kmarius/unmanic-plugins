from typing import TypedDict, Callable


class FileQueuedData(TypedDict):
    library_id: int
    file_path: str
    priority_score: int
    issues: list


class PostprocessorCompleteData(TypedDict):
    library_id: int
    task_id: int
    task_type: str
    source_data: dict
    destination_data: dict
    task_success: bool
    start_time: float
    finish_time: float
    processed_by_worker: str
    log: str


class PostprocessorStartedData(TypedDict):
    library_id: int
    task_id: int
    task_type: str
    cache_path: str
    source_data: dict


class ScanCompleteData(TypedDict):
    library_id: int
    library_name: str
    library_path: str
    scan_start_time: float
    scan_end_time: float
    scan_duration: float
    files_scanned_count: int


class TaskQueuedData(TypedDict):
    library_id: int
    task_id: int
    task_type: str
    source_data: dict


class TaskScheduledData(TypedDict):
    library_id: int
    task_id: int
    task_type: str
    task_schedule_type: str
    remote_installation_info: dict
    source_data: dict


class WorkerProcessCompleteData(TypedDict):
    library_id: int
    task_id: int
    task_type: str
    original_file_path: str
    final_cache_path: str
    overall_success: bool
    worker_runners_info: dict
    worker_log: list


class WorkerProcessStartedData(TypedDict):
    library_id: int
    task_id: int
    task_type: str
    original_file_path: str
    task_cache_path: str
    worker_runners_info: dict


class PanelData(TypedDict):
    content_type: str
    content: str
    path: str
    arguments: dict


class PluginApiData(TypedDict):
    content_type: str
    content: dict
    status: int
    method: str
    path: str
    uri: str
    query: str
    arguments: dict
    body: bytes


class FileTestData(TypedDict):
    library_id: int
    path: str
    issues: list
    add_file_to_pending_tasks: bool
    priority_score: int
    shared_info: dict


class FileMoveData(TypedDict):
    library_id: int
    source_data: dict
    remove_source_file: bool
    copy_file: bool
    file_in: str
    file_out: str
    run_default_file_copy: bool


class TaskResultData(TypedDict):
    final_cache_path: str
    library_id: int
    task_processing_success: bool
    file_move_processes_success: bool
    destination_files: list
    source_data: dict


class ProcessItemData(TypedDict):
    worker_log: list
    library_id: int
    exec_command: list[str]
    command_progress_parser: Callable[[str], dict]
    file_in: str
    file_out: str
    original_file_path: str
    repeat: bool