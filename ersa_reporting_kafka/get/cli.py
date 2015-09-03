#!/usr/bin/env python
"""
Message retrieval.
"""

# pylint: disable=import-self

import json
import sys
import os

from .. import REQUIRED_ENVIRONMENT_REPORTING, api

COMMAND = "get"
DESCRIPTION = "Retrieve [batch size] of messages @ (topic, partition, offset)."

SCHEMA = COMMAND


def setup(subparser):
    """Get CLI setup."""
    subparser.add_argument("--topic",
                           required=True,
                           help="kafka-reporting topic")
    subparser.add_argument("--partition",
                           required=True,
                           type=int,
                           help="kafka-reporting partition")
    subparser.add_argument(
        "--offset",
        required=True,
        type=int,
        help="kafka-reporting offset (negative → relative to most recent)")
    subparser.add_argument(
        "--pretty",
        help="Pretty-print JSON.",
        action="store_true")


def execute(args):
    """Get execution."""
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

    data, _ = api.API(**api_config).get(args.topic, args.partition,
                                        args.offset)

    if data:
        if args.pretty:
            print(json.dumps(data, indent=4, sort_keys=True))
        else:
            print(json.dumps(data))
