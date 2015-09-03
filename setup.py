#!/usr/bin/env python

# pylint: disable=missing-docstring

from setuptools import setup

setup(
    name="ersa-reporting-kafka",
    version="0.1",
    install_requires=["arrow", "requests", "python-swiftclient",
                      "python-keystoneclient", "python-cinderclient",
                      "python-novaclient", "python-ceilometerclient",
                      "swift>=2.0.0", "backports.lzma", "tabulate"],
    packages=["ersa_reporting_kafka", "ersa_reporting_kafka.api",
              "ersa_reporting_kafka.archive",
              "ersa_reporting_kafka.hello_world", "ersa_reporting_kafka.nova",
              "ersa_reporting_kafka.cinder", "ersa_reporting_kafka.keystone",
              "ersa_reporting_kafka.swift", "ersa_reporting_kafka.ceilometer",
              "ersa_reporting_kafka.status", "ersa_reporting_kafka.get"],
    scripts=["bin/ersa-kr"])
