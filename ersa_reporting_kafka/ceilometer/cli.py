#!/usr/bin/env python
"""
ceilometer -> reporting
"""

# pylint: disable=import-self,protected-access

import os
import sys
import time

from functools import partial
from multiprocessing.pool import ThreadPool

from .. import REQUIRED_ENVIRONMENT_REPORTING
from .. import REQUIRED_ENVIRONMENT_OPENSTACK
from .. import api

import arrow

from ceilometerclient import client

COMMAND = "ceilometer"
DESCRIPTION = "Producer: ceilometer samples."

SCHEMA = COMMAND

PAGE_SIZE = 2500


def setup(subparser):
    """ceilometer CLI setup."""
    subparser.add_argument("--topic",
                           required=True,
                           help="kafka-reporting topic")
    subparser.add_argument("--start", required=True, help="Start timestamp")
    subparser.add_argument("--end", required=True, help="End timestamp")


def _parse_timestamp(timestamp):
    """
    Parse three different types of timestamp:
    - "now"
    - "now-X" where X is seconds
    - "X" where X is anything that arrow.get(X) will handle
    """
    if timestamp == "now":
        return arrow.now()
    elif timestamp.startswith("now-"):
        delta = int(timestamp.split("-")[1])
        return arrow.get(time.time() - delta)
    else:
        return arrow.get(timestamp)


def _chunks(data, chunk_size):
    """Break a list into chunks of size n."""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]


def _upload(samples, kafka, topic):
    """kafka upload function."""
    kafka.put(topic, SCHEMA, {"samples": samples})


def execute(args):
    """ceilometer execution."""
    missing_environment = [
        var for var in (REQUIRED_ENVIRONMENT_REPORTING +
                        REQUIRED_ENVIRONMENT_OPENSTACK)
        if var not in os.environ
    ]

    if len(missing_environment) > 0:
        sys.exit("Missing environment variables: %s" %
                 " ".join(missing_environment))

    start_timestamp = _parse_timestamp(args.start)
    end_timestamp = _parse_timestamp(args.end)

    api_config = {
        "server": os.getenv("REPORTING_SERVER"),
        "username": os.getenv("REPORTING_USERNAME"),
        "token": os.getenv("REPORTING_TOKEN"),
        "https_verify": not args.insecure
    }

    ceilometer = client.get_client(2,
                                   os_auth_url=os.getenv("OS_AUTH_URL"),
                                   os_username=os.getenv("OS_USERNAME"),
                                   os_password=os.getenv("OS_PASSWORD"),
                                   os_tenant_name=os.getenv("OS_TENANT_NAME"))

    query = [
        {"field": "timestamp",
         "op": "ge",
         "value": start_timestamp},
        {"field": "timestamp",
         "op": "le",
         "value": end_timestamp}
    ]

    result = [sample.to_dict()
              for sample in ceilometer.new_samples.list(q=query)]

    result_chunks = list(_chunks(result, PAGE_SIZE))

    kafka = api.API(**api_config)
    kafka_upload = partial(_upload, kafka=kafka, topic=args.topic)
    ThreadPool().map(kafka_upload, result_chunks, 1)
