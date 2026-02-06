import json
import subprocess
from typing import Optional

from .ffmpeg.probe import Probe
from . import logger


class MetadataProvider:
    name = "None"
    """Used as table name and field name in the shared_info dict."""

    default_enabled = False

    @staticmethod
    def run_prog(path: str) -> Optional[dict]:
        raise NotImplementedError()


class FFprobeProvider(MetadataProvider):
    name = "ffprobe"
    default_enabled = True

    @staticmethod
    def run_prog(path: str) -> Optional[dict]:
        probe = Probe(logger)
        if not probe.file(path):
            return None
        return probe.get_probe()


class MediaInfoProvider(MetadataProvider):
    name = "mediainfo"

    @staticmethod
    def run_prog(path: str) -> Optional[dict]:
        try:
            command = ["mediainfo", "--output=JSON", path]
            pipe = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            out, err = pipe.communicate()

            return json.loads(out.decode("utf-8"))
        except Exception as e:
            logger.error(e)
            return None


PROVIDERS = [
    FFprobeProvider,
    MediaInfoProvider,
]