#!/usr/bin/env python
"""
cinder -> reporting
"""

# pylint: disable=import-self,protected-access

import os
import sys
import uuid

from .. import REQUIRED_ENVIRONMENT_REPORTING
from .. import REQUIRED_ENVIRONMENT_OPENSTACK
from .. import api

from keystoneclient.auth.identity import v2
from keystoneclient import session
from cinderclient import client

COMMAND = "cinder"
DESCRIPTION = "Producer: cinder volumes."

SCHEMA = COMMAND

PAGE_SIZE = 1000


def setup(subparser):
    """cinder CLI setup."""
    subparser.add_argument("--topic",
                           required=True,
                           help="kafka-reporting topic")


def _volumes(cinder, marker=None):
    """cinder list"""
    return cinder.volumes.list(search_opts={"all_tenants": 1},
                               marker=marker,
                               limit=PAGE_SIZE)


def execute(args):
    """cinder execution."""
    missing_environment = [
        var for var in (REQUIRED_ENVIRONMENT_REPORTING +
                        REQUIRED_ENVIRONMENT_OPENSTACK)
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

    auth = v2.Password(auth_url=os.getenv("OS_AUTH_URL"),
                       username=os.getenv("OS_USERNAME"),
                       password=os.getenv("OS_PASSWORD"),
                       tenant_name=os.getenv("OS_TENANT_NAME"))
    keystone_session = session.Session(auth=auth)
    cinder = client.Client(2, session=keystone_session)

    kafka = api.API(**api_config)

    snapshot = str(uuid.uuid4())

    volumes = _volumes(cinder)

    while len(volumes) > 0:
        payload = {
            "snapshot": snapshot,
            "volumes": [volume._info for volume in volumes]
        }

        kafka.put(args.topic, SCHEMA, payload)

        volumes = _volumes(cinder, marker=volumes[-1].id)
