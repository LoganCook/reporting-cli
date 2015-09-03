#!/usr/bin/env python
"""
keystone -> reporting
"""

# pylint: disable=broad-except,import-self,protected-access

import os
import sys
import time

from multiprocessing.pool import ThreadPool

from .. import REQUIRED_ENVIRONMENT_REPORTING
from .. import REQUIRED_ENVIRONMENT_OPENSTACK
from .. import api

from keystoneclient.auth.identity import v2
from keystoneclient.v2_0 import client
from keystoneclient import session

COMMAND = "keystone"
DESCRIPTION = "Producer: keystone users, tenants and memberships."

SCHEMA = COMMAND


def setup(subparser):
    """keystone CLI setup."""
    subparser.add_argument("--topic",
                           required=True,
                           help="kafka-reporting topic")


def process(tenant):
    """Populate tenants."""
    tenant_info = tenant._info
    if tenant_info[
            "description"
    ] and "personal tenancy" not in tenant_info["description"].lower():
        membership_retrieved = False
        membership_attempt = 0
        while not membership_retrieved and membership_attempt < 3:
            try:
                tenant_info["users"] = [
                    user.id for user in tenant.list_users()
                ]
                membership_retrieved = True
            except Exception as exception:
                print(exception)
                time.sleep(2 ** membership_attempt)
                membership_attempt += 1
    return tenant_info


def execute(args):
    """keystone execution."""
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
    keystone = client.Client(session=keystone_session)

    kafka = api.API(**api_config)

    users = [user._info for user in keystone.users.list()]

    tenants = ThreadPool().map(process, keystone.tenants.list())

    payload = {"users": users, "tenants": tenants}

    kafka.put(args.topic, SCHEMA, payload)
