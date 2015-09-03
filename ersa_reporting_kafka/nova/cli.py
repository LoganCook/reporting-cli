#!/usr/bin/env python
"""
nova -> reporting
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
from novaclient import client

COMMAND = "nova"
DESCRIPTION = "Producer: nova flavors and instances."

SCHEMA = COMMAND

PAGE_SIZE = 1000


def setup(subparser):
    """nova CLI setup."""
    subparser.add_argument("--topic",
                           required=True,
                           help="kafka-reporting topic")


def _instances(nova, marker=None):
    """nova list"""
    return nova.servers.list(search_opts={"all_tenants": 1},
                             marker=marker,
                             limit=PAGE_SIZE)


def _flavors(nova):
    """nova flavor list"""
    return nova.flavors.list(is_public=None)


def execute(args):
    """nova execution."""
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
    nova = client.Client(2, session=keystone_session)

    kafka = api.API(**api_config)

    snapshot = str(uuid.uuid4())

    flavors = [f._info for f in _flavors(nova)]

    instances = _instances(nova)

    while len(instances) > 0:
        payload = {
            "snapshot": snapshot,
            "flavors": flavors,
            "instances": [instance._info for instance in instances]
        }

        kafka.put(args.topic, SCHEMA, payload)

        instances = _instances(nova, marker=instances[-1].id)
