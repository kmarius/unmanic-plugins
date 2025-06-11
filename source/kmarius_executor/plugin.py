#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os

from kmarius.lib import lazy_init
from kmarius.lib.ffmpeg import StreamMapper, Parser

# Configure plugin logger
logger = logging.getLogger("Unmanic.Plugin.kmarius_executor")

# we use this to pass data from the tester to the processor
kmarius_data = {}


class PluginStreamMapper(StreamMapper):
    def __init__(self, mappings):
        super(PluginStreamMapper, self).__init__(
            logger, ['audio', "video", "subtitle", "data", "attachment"])
        self.mappings = mappings
        self.settings = None

    def set_default_values(self, settings, abspath, probe):
        self.abspath = abspath
        self.set_probe(probe)
        self.set_input_file(abspath)
        self.settings = settings

    def test_stream_needs_processing(self, stream_info: dict):
        stream_type = stream_info.get('codec_type', '').lower()
        if not stream_type in self.mappings:
            # no mappings for this type of stream
            return False
        return stream_info["idx"] in self.mappings[stream_type]

    def custom_stream_mapping(self, stream_info: dict, stream_id: int):
        stream_type = stream_info.get('codec_type', '').lower()
        return self.mappings[stream_type][stream_id]


def on_library_management_file_test(data):
    kmarius = lazy_init(data, logger)
    path = data.get("path")

    if kmarius["add_file_to_pending_tasks"]:
        data['add_file_to_pending_tasks'] = True
        # pass data to the processor via global variable
        global kmarius_data
        kmarius_data[path] = kmarius

    return data


def on_worker_process(data):
    path = data.get("original_file_path")
    if not path in kmarius_data:
        logger.error(f"no data for {path}")
        data["exec_command"] = []
        return data

    kmarius = kmarius_data[path]
    del kmarius_data[path]

    probe = kmarius.get("probe")
    abspath = data.get('file_in')

    mapper = PluginStreamMapper(kmarius.get("mappings", {}))
    mapper.set_default_values(None, abspath, probe)

    needs_remux = kmarius.get("needs_remux", False)
    needs_moov = kmarius.get("moov_to_front", False)

    if mapper.streams_need_processing() or needs_remux or needs_moov:
        mapper.set_input_file(abspath)

        if needs_remux:
            split_file_out = os.path.splitext(data.get('file_out'))
            new_file_out = "{}.{}".format(split_file_out[0], "mp4")
            mapper.set_output_file(new_file_out)
            data['file_out'] = new_file_out
        else:
            mapper.set_output_file(data.get('file_out'))

        #  "-map", "-0:t", used in old script but fails here
        mapper.main_options += ["-map_metadata", "-1", "-map_chapters", "-1"]

        # we always want moov at front
        mapper.main_options += ["-movflags", "+faststart"]

        ffmpeg_args = mapper.get_ffmpeg_args()

        data['exec_command'] = ['ffmpeg']
        data['exec_command'] += ffmpeg_args

        parser = Parser(logger)
        parser.set_probe(probe)
        data['command_progress_parser'] = parser.parse_progress

    return data