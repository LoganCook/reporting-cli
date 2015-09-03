#!/usr/bin/env python
"""
Archive messages from the kafka-reporting API into OpenStack Swift.
"""

# pylint: disable=import-self

import sys
import os

from .. import REQUIRED_ENVIRONMENT_REPORTING, REQUIRED_ENVIRONMENT_OPENSTACK
from . import Archive, Stream

COMMAND = "archive"
DESCRIPTION = "Consumer: archive content into swift."


def setup(subparser):
    """Archive CLI setup."""
    subparser.add_argument("--topic",
                           required=True,
                           help="kafka-reporting topic")
    subparser.add_argument("--partition",
                           type=int,
                           required=True,
                           help="kafka-reporting partition")
    subparser.add_argument("--container",
                           required=True,
                           help="swift container")
    subparser.add_argument("--prefix",
                           default="",
                           help="swift object prefix (default '')")
    subparser.add_argument("--offset",
                           type=int,
                           help="override start offset (default automatic)")


def execute(args):
    """Archive CLI execution."""
    missing_environment = [
        var for var in (REQUIRED_ENVIRONMENT_REPORTING +
                        REQUIRED_ENVIRONMENT_OPENSTACK)
        if var not in os.environ
    ]

    if len(missing_environment) > 0:
        sys.exit("Missing environment variables: %s" %
                 " ".join(missing_environment))

    swift_config = {
        "auth_version": 2,
        "authurl": os.getenv("OS_AUTH_URL"),
        "user": os.getenv("OS_USERNAME"),
        "key": os.getenv("OS_PASSWORD"),
        "tenant_name": os.getenv("OS_TENANT_NAME")
    }

    archive = Archive(swift_config, args.container, args.prefix)

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
