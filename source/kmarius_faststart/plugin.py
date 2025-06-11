#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os
import subprocess

from kmarius.lib import lazy_init

logger = logging.getLogger("Unmanic.Plugin.kmarius_faststart_handler")


def moov_is_at_front(path):
    command = ["ffprobe", "-v", "trace", path]
    pipe = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out, err = pipe.communicate()

    output = out.decode("utf-8")
    moov_idx = output.find("moov")
    mdat_idx = output.find("mdat")

    # apparently the output of ffprobe is not always reliable w.r.t atoms
    if moov_idx == -1 or mdat_idx == -1:
        return False

    return moov_idx < mdat_idx


def on_library_management_file_test(data):
    kmarius = lazy_init(data, logger)
    path = data.get("path")

    split_file_in = os.path.splitext(path)
    extension = split_file_in[1].lstrip(".").lower()

    # we only check mp4 containers, if e.g. an mkv is remuxed to mp4, moov will always be moved to front
    if extension == "mp4" and not moov_is_at_front(path):
        data["issues"].append({
            'id': "kmarius_faststart_handler",
            'message': f"MOOV atom not at front: {path}",
        })
        kmarius["moov_to_front"] = True
        kmarius["add_file_to_pending_tasks"] = True

    return data