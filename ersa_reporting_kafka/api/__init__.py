#!/usr/bin/env python
"""Kafka-Reporting API interaction."""

import json
import logging
import platform
import time
import uuid

from multiprocessing.pool import ThreadPool

import arrow
import requests

REPORTING_RETRY_LIMIT = 10
SESSION = str(uuid.uuid4())


class API:
    """Fetch messages from the kafka-reporting API."""

    def __init__(self, server, username, token, https_verify=True):
        self.server = server
        self.username = username
        self.token = token
        self.https_verify = https_verify

    def _parse_partitions(self, topic, partitions):
        """Parse partition metadata."""
        metadata = {
            "messages": 0,
            "earliest_timestamp": None,
            "latest_timestamp": None,
            "earliest_offsets": ["-" for _ in range(len(partitions))],
            "latest_offsets": ["-" for _ in range(len(partitions))]
        }
        for partition_id, partition_metadata in partitions.items():
            partition_id = int(partition_id)
            partition_size = partition_metadata[
                "latestOffset"
            ] - partition_metadata["earliestOffset"]
            metadata["messages"] += partition_size
            if partition_size > 0:
                metadata["earliest_offsets"][
                    partition_id
                ] = partition_metadata["earliestOffset"]

                earliest = self.get(
                    topic, int(partition_id),
                    partition_metadata["earliestOffset"])[0]
                if earliest:
                    message = earliest[0]
                    when = arrow.get(message["timestamp"] / 1000)

                    if not metadata["earliest_timestamp"]:
                        metadata["earliest_timestamp"] = when
                    else:
                        metadata["earliest_timestamp"] = min(
                            metadata["earliest_timestamp"], when)

                metadata["latest_offsets"][partition_id] = partition_metadata[
                    "latestOffset"
                ]

                latest = self.get(topic, int(partition_id), -1)[0]
                if latest:
                    message = latest[0]
                    when = arrow.get(message["timestamp"] / 1000)

                    if not metadata["latest_timestamp"]:
                        metadata["latest_timestamp"] = when
                    else:
                        metadata["latest_timestamp"] = max(
                            metadata["latest_timestamp"], when)
        return metadata

    def _fetch_topic_metadata(self, topic):
        """Fetch general information about topic."""
        topic_url = "https://%s/v1/topic/%s" % (self.server, topic)
        topic_response = requests.get(topic_url,
                                      auth=(self.username, self.token),
                                      verify=self.https_verify)
        if topic_response.status_code != 200:
            raise IOError("topic retrieval failure (%s): HTTP %i" %
                          (topic, topic_response.status_code))
        partitions = topic_response.json()["partition"]
        return self._parse_partitions(topic, partitions)

    def list(self, threads=1):
        """Return topic metadata."""
        list_url = "https://%s/v1/topic" % self.server
        list_response = requests.get(list_url,
                                     auth=(self.username, self.token),
                                     verify=self.https_verify)
        if list_response.status_code != 200:
            raise IOError(
                "topic list failure: HTTP %i" % list_response.status_code)
        topics = {}
        topic_names = list_response.json()["topics"]
        topic_metadata = ThreadPool(threads).map(self._fetch_topic_metadata,
                                                 topic_names)
        for name, metadata in zip(topic_names, topic_metadata):
            topics[name] = metadata

        return topics

    def get(self, topic, partition, offset):
        """Perform one fetch operation from the given offset."""
        url = "https://%s/v1/topic/%s/%d/%d" % (
            self.server, topic, partition, offset)

        retry = 0
        while retry < REPORTING_RETRY_LIMIT:
            response = requests.get(
                url,
                auth=(self.username, self.token),
                verify=self.https_verify)
            if response.status_code == 200:
                data = response.json()
                break
            elif response.status_code >= 400 and response.status_code < 500:
                raise RuntimeError("http error %i" % response.status_code)
            else:
                logging.warning("http error %i; will retry (#%i)",
                                response.status_code, retry + 1)
                time.sleep(2 * 2 ** retry)
                retry += 1

        if retry == REPORTING_RETRY_LIMIT:
            raise IOError("reached retry limit: giving up")

        if len(data["messages"]) > 0:
            result = [entry["message"] for entry in data["messages"]]
            next_offset = data["messages"][-1]["next_offset"]
            return result, next_offset
        else:
            return None, None

    def put(self, topic, schema, data):
        """Post a single message to the API."""

        url = "https://%s/v1/topic/%s" % (self.server, topic)

        data = data.copy()
        data["timestamp"] = int(time.time())
        data["hostname"] = platform.node()

        message = json.dumps([{
            "id": str(uuid.uuid4()),
            "session": SESSION,
            "schema": schema,
            "version": 1,
            "data": data
        }])

        retry = 0
        while retry < REPORTING_RETRY_LIMIT:
            response = requests.post(
                url,
                auth=(self.username, self.token),
                headers={"content-type": "application/json"},
                data=message,
                verify=self.https_verify)

            if response.status_code == 204:
                break
            else:
                logging.warning("http error %i; will retry (#%i)",
                                response.status_code, retry + 1)
                time.sleep(2 * 2 ** retry)
                retry += 1

        if retry == REPORTING_RETRY_LIMIT:
            raise IOError("reached retry limit: giving up")
