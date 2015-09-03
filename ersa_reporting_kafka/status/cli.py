#!/usr/bin/env python
"""
kafka status
"""

# pylint: disable=import-error,import-self,unused-argument

import sys
import os

from tabulate import tabulate

from .. import REQUIRED_ENVIRONMENT_REPORTING, api

COMMAND = "status"
DESCRIPTION = "Show kafka status."

SCHEMA = COMMAND

HEADING = ["Topic", "Window Size", "Earliest", "Latest"]


def setup(subparser):
    """Status CLI setup."""
    pass


def _printable(timestamp, offsets):
    """Generate a printable timestamp/offset string."""
    if not timestamp:
        return "-"

    timestamp_offset_string = "[%s]" % " ".join([str(o) for o in offsets])
    return "%s %s" % (timestamp.humanize(), timestamp_offset_string)


def execute(args):
    """Status execution."""
    missing_environment = [
        var for var in (REQUIRED_ENVIRONMENT_REPORTING)
        if var not in os.environ
    ]

    if len(missing_environment) > 0:
        sys.exit("Missing environment variables: %s" %
                 " ".join(missing_environment))

    api_config = {
        "server": os.getenv("REPORTING_SERVER"),
        "username": os.getenv("REPORTING_USERNAME"),
        "token": os.getenv("REPORTING_TOKEN"),
        "https_verify": not args.insecure
    }

    status = api.API(**api_config).list()

    records = []

    topics = sorted(list(status.keys()))

    for topic in topics:
        metadata = status[topic]
        earliest = _printable(metadata["earliest_timestamp"],
                              metadata["earliest_offsets"])
        latest = _printable(metadata["latest_timestamp"],
                            metadata["latest_offsets"])
        records.append([topic, "{:,}".format(metadata["messages"]), earliest,
                        latest])

    print(tabulate(records, headers=HEADING))
