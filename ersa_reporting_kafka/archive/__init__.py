#!/usr/bin/env python2
"""
Archive messages from the kafka-reporting API into OpenStack Swift.
"""

# pylint: disable=import-error,too-many-arguments
# pylint: disable=import-self,too-few-public-methods

import io
import json
import re

from backports import lzma
from boto.s3.connection import S3Connection

from .. import api

# HCP #facepalm
import ssl
if hasattr(ssl, '_create_unverified_context'):
    ssl._create_default_https_context = ssl._create_unverified_context

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
    """Handle all interactions (save, list) with Object Store through Boto."""

    # clustername/topic/partition/offset1-offset2.json.xz
    re_offset = re.compile(r".*/[0-9]+-([0-9]+)\..+")

    def __init__(self, aws_id, aws_secret, server, bucket, object_prefix=""):
        aws_s3 = S3Connection(aws_access_key_id=aws_id,
                              aws_secret_access_key=aws_secret,
                              host=server)
        self.bucket = aws_s3.get_bucket(bucket)
        self.object_prefix = object_prefix

        if len(object_prefix) > 0 and not object_prefix.endswith("/"):
            self.object_prefix += "/"

    def save(self, topic, partition, start_offset, end_offset, content):
        """Save an object in object store."""
        dump_name = "%s%s/%s/%s-%s.json.xz" % (
            self.object_prefix, topic, partition, str(start_offset).zfill(12),
            str(end_offset).zfill(12))

        self.bucket.new_key(dump_name).set_contents_from_string(content)

    def latest(self, topic, partition):
        """Find the highest kafka-reporting offset in object store."""
        list_all = self.bucket.list("%s%s/%i/" % (
            self.object_prefix, topic, partition))

        end_offset = 0
        for item in list_all:
            try:
                offset = int(self.re_offset.match(item.name).group(1))
                if offset > end_offset:
                    end_offset = offset
            except AttributeError:
                pass
        return end_offset


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
