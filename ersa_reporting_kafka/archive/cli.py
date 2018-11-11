#!/usr/bin/env python
"""
Archive messages from the kafka-reporting API into object store on AWS.
"""

# pylint: disable=import-self

import sys
import os

from .. import REQUIRED_ENVIRONMENT_REPORTING, REQUIRED_ENVIRONMENT_AWS
from . import Archive, Stream

COMMAND = "archive"
DESCRIPTION = "Consumer: archive content into object store."


def setup(subparser):
    """Archive CLI setup."""
    subparser.add_argument("--topic",
                           required=True,
                           help="kafka-reporting topic")
    subparser.add_argument("--partition",
                           type=int,
                           required=True,
                           help="kafka-reporting partition")
    subparser.add_argument("--namespace",
                           required=True,
                           help="AWS namespace")
    subparser.add_argument("--prefix",
                           default="",
                           help="object prefix (default '')")
    subparser.add_argument("--offset",
                           type=int,
                           help="override start offset (default automatic)")


def execute(args):
    """Archive CLI execution."""
    missing_environment = [
        var for var in (REQUIRED_ENVIRONMENT_REPORTING +
                        REQUIRED_ENVIRONMENT_AWS)
        if var not in os.environ
    ]

    if len(missing_environment) > 0:
        sys.exit("Missing environment variables: %s" %
                 " ".join(missing_environment))

    aws_id = os.getenv("OS_AWS_ID")
    aws_secret = os.getenv("OS_AWS_SECRET")
    aws_server = os.getenv("OS_AWS_URL")
    archive = Archive(aws_id,
                      aws_secret,
                      aws_server,
                      args.namespace,
                      args.prefix)

    stream_config = {
        "server": os.getenv("REPORTING_SERVER"),
        "username": os.getenv("REPORTING_USERNAME"),
        "token": os.getenv("REPORTING_TOKEN"),
        "archive": archive,
        "https_verify": not args.insecure
    }

    stream = Stream(**stream_config)

    latest_stored_offset = archive.latest(
        args.topic, args.partition) if not args.offset else args.offset
    if latest_stored_offset > 0 and not args.offset:
        latest_stored_offset += 1

    stream.stream(args.topic, args.partition, latest_stored_offset)
