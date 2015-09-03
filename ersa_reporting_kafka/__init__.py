#!/usr/bin/env python
"""Global housekeeping."""

import logging

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

REQUIRED_ENVIRONMENT_REPORTING = [
    "REPORTING_%s" % suffix for suffix in ["SERVER", "USERNAME", "TOKEN"]
]

REQUIRED_ENVIRONMENT_OPENSTACK = [
    "OS_%s" % suffix
    for suffix in ["AUTH_URL", "USERNAME", "PASSWORD", "TENANT_NAME"]
]
