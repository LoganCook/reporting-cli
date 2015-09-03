#!/usr/bin/env python
"""
Archive messages from the kafka-reporting API into OpenStack Swift.
"""

# pylint: disable=import-error,too-many-arguments
# pylint: disable=import-self,too-few-public-methods

import io
import json
import re

import swiftclient

from backports import lzma

from .. import api

REPORTING_RETRY_LIMIT = 10


class Dump:
    """An LZMA-compressing buffer for incrementally-built JSON arrays."""

    def __init__(self):
        self.empty = True
        self.buffer = io.BytesIO()
        self.dump = lzma.open(self.buffer, mode="wt", preset=9)
        self.dump.write(u"[")

    def write(self, item):
        """Add an entry to the array."""
        if self.empty:
            self.empty = False
        else:
            self.dump.write(u",")
        return self.dump.write(json.dumps(item).decode())

    def finish(self):
        """Close the array and return the compressed result."""
        self.dump.write(u"]")
        self.dump.close()
        return self.buffer.getvalue()


class Archive:
    """Handle all interactions (save, list) with Swift."""

    re_offset = re.compile("[0-9]+")

    def __init__(self, swift_config, container, object_prefix=""):
        self.swift = swiftclient.client.Connection(**swift_config)
        self.container = container
        self.object_prefix = object_prefix

        if len(object_prefix) > 0 and not object_prefix.endswith("/"):
            self.object_prefix += "/"

    def save(self, topic, partition, start_offset, end_offset, content):
        """Save an object in Swift."""
        dump_name = "%s%s/%s/%s-%s.json.xz" % (
            self.object_prefix, topic, partition, str(start_offset).zfill(12),
            str(end_offset).zfill(12))

        self.swift.put_object(self.container, dump_name, content)

    def latest(self, topic, partition):
        """Find the highest kafka-reporting offset in Swift."""
        list_all = self.swift.get_container(
            self.container,
            prefix="%s%s/%i/" % (self.object_prefix, topic, partition),
            full_listing=True)
        list_names = [_["name"] for _ in list_all[1]]
        end_offsets = [int(self.re_offset.findall(_)[-1]) for _ in list_names]

        if len(end_offsets) == 0:
            return 0
        else:
            return max(end_offsets)


class Stream:
    """Handle all interactions with the kafka-reporting API."""

    def __init__(self, server, username, token, archive, https_verify=True):
        self.archive = archive
        self.api = api.API(server, username, token, https_verify)

    def stream(self, topic, partition, offset=0):
        """Fetch all messages from the given offset up to the latest offset."""
        start_offset = offset

        items_written = 0
        bytes_written = 0

        dump = Dump()

        while True:
            data, next_offset = self.api.get(topic, partition, offset)

            if data is None:
                if not dump.empty:
                    self.archive.save(topic, partition, start_offset,
                                      start_offset + items_written - 1,
                                      dump.finish())
                break

            for item in data:
                bytes_written += dump.write(item)
                items_written += 1

            if bytes_written >= 256 * 1024 * 1024:
                self.archive.save(topic, partition, start_offset,
                                  start_offset + items_written - 1,
                                  dump.finish())

                dump = Dump()
                items_written = 0
                bytes_written = 0
                start_offset = next_offset

            offset = next_offset
