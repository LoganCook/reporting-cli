#!/usr/bin/env python
"""
insightly -> reporting
"""

# pylint: disable=import-self

import sys
import os

import requests

from .. import REQUIRED_ENVIRONMENT_REPORTING, api

COMMAND = "crm"
DESCRIPTION = "Producer: Insightly CRM."

REQUIRED_ENVIRONMENT = REQUIRED_ENVIRONMENT_REPORTING + ["INSIGHTLY_TOKEN"]

SCHEMA = COMMAND


def setup(subparser):
    """Insightly CRM setup."""
    subparser.add_argument("--topic",
                           required=True,
                           help="kafka-reporting topic")


def _get(crm_type, auth):
    """Fetch all records of single data type."""
    response = requests.get("https://api.insight.ly/v2.1/%s" % crm_type,
                            auth=(auth, ""),
                            headers={"accept-encoding": "gzip"})
    if response.status_code == 200:
        return response.json()
    else:
        return None


def _extract_contacts(raw):
    """Create a simplified contacts data structure."""

    contacts = []

    for contact in raw:
        username = [custom["FIELD_VALUE"] for custom in contact["CUSTOMFIELDS"]
                    if custom["CUSTOM_FIELD_ID"] == "CONTACT_FIELD_5"]

        username = username[0] if (len(username) > 0) else None

        email = [info["DETAIL"] for info in contact["CONTACTINFOS"]
                 if info["TYPE"] == "EMAIL"]

        links = [link["ORGANISATION_ID"] for link in contact["LINKS"]
                 if link["ORGANISATION_ID"] is not None]

        first_name = contact["FIRST_NAME"] if contact["FIRST_NAME"] else ""
        last_name = contact["LAST_NAME"] if contact["LAST_NAME"] else ""

        summary = {
            "id": contact["CONTACT_ID"],
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "username": username,
            "organisations": links
        }

        contacts.append(summary)

    return contacts


def _extract_organisations(raw):
    """Create a simplified organisations data structure."""

    organisations = []

    for organisation in raw:
        summary = {
            "id": organisation["ORGANISATION_ID"],
            "name": organisation["ORGANISATION_NAME"]
        }

        organisations.append(summary)

    return organisations


def _retrieve(auth):
    """Fetch the latest CRM data."""

    contacts = _extract_contacts(_get("contacts", auth))
    organisations = _extract_organisations(_get("organisations", auth))

    return {"contacts": contacts, "organisations": organisations}


def execute(args):
    """Insightly CRM execution."""
    missing_environment = [
        var for var in REQUIRED_ENVIRONMENT if var not in os.environ
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

    insightly = os.getenv("INSIGHTLY_TOKEN")

    api.API(**api_config).put(args.topic, SCHEMA, _retrieve(insightly))
