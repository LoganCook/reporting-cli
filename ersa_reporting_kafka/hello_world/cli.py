#!/usr/bin/env python
"""
hello world -> reporting
"""

# pylint: disable=import-self

import sys
import os

from .. import REQUIRED_ENVIRONMENT_REPORTING, api

COMMAND = "hello-world"
DESCRIPTION = "Push a simple Hello World message into the system."

SCHEMA = COMMAND


def setup(subparser):
    """Hello World CLI setup."""
    subparser.add_argument("--topic",
                           required=True,
                           help="kafka-reporting topic")


def execute(args):
    """Hello World execution."""
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

    api.API(**api_config).put(args.topic, SCHEMA,
                              {"hello": "world",
                               "answer": 42})
