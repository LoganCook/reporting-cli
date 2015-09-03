#!/usr/bin/env python
"""
swift -> reporting
"""

# pylint: disable=import-error,import-self
# pylint: disable=protected-access,broad-except,unused-variable

import logging
import os
import random
import sys

from functools import partial
from multiprocessing.pool import ThreadPool

from .. import REQUIRED_ENVIRONMENT_REPORTING
from .. import REQUIRED_ENVIRONMENT_OPENSTACK
from .. import api

import requests

from keystoneclient.v2_0 import client
from swift.common.ring import Ring

COMMAND = "swift"
DESCRIPTION = "Producer: swift usage by tenant."

SCHEMA = COMMAND


def setup(subparser):
    """swift CLI setup."""
    subparser.add_argument("--topic",
                           required=True,
                           help="kafka-reporting topic")
    subparser.add_argument("--ring", required=True, help="account ring file")


def _fetch(tenant, ring):
    """Fetch tenant usage."""
    account = "AUTH_%s" % tenant
    partition = ring.get_part(account, None, None)
    nodes = ring.get_part_nodes(partition)
    random.shuffle(nodes)
    for node in nodes:
        url = "http://%s:%s/%s/%s/%s" % (node["ip"], node["port"],
                                         node["device"], partition, account)
        try:
            response = requests.head(url, timeout=5)
            if response.status_code == 204:
                return {
                    "containers":
                    int(response.headers["x-account-container-count"]),
                    "objects": int(response.headers["x-account-object-count"]),
                    "bytes": int(response.headers["x-account-bytes-used"]),
                    "quota":
                    int(response.headers["x-account-meta-quota-bytes"]) if
                    "x-account-meta-quota-bytes" in response.headers else None
                }
            elif response.status_code == 404:
                return None
            else:
                logging.warning("error fetching %s [HTTP %s]", url,
                                response.status_code)
        except Exception as _exception:  # noqa
            logging.warning("error fetching %s", url, exc_info=True)
    raise IOError("failed to fetch info for tenant %s", tenant)


def execute(args):
    """swift execution."""
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

    keystone_config = {
        "username": os.getenv("OS_USERNAME"),
        "password": os.getenv("OS_PASSWORD"),
        "tenant_name": os.getenv("OS_TENANT_NAME"),
        "auth_url": os.getenv("OS_AUTH_URL")
    }
    keystone = client.Client(**keystone_config)

    ring = Ring(args.ring)

    kafka = api.API(**api_config)

    fetch = partial(_fetch, ring=ring)

    tenants = [tenant.id for tenant in keystone.tenants.list()]
    random.shuffle(tenants)

    report = {}
    for tenant, stats in zip(tenants, ThreadPool().map(fetch, tenants)):
        report[tenant] = stats

    kafka.put(args.topic, SCHEMA, report)
