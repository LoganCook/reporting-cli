# reporting-cli [![Build Status](https://travis-ci.org/eResearchSA/reporting-cli.svg)](https://travis-ci.org/eResearchSA/reporting-cli)
Kafka/Reporting CLI. It has modules to:

1. get/put content from/to Kafka
1. act as producers to generate content
1. archive content to S3 like object stores

## Library dependencies

Run this command to install them:

`sudo apt-get intall -y python-dev liblzma-dev`

## Runtime environment variables
1. Kafka related which are defined with prefix of `REPORTING_`:

    REPORTING_SERVER, REPORTING_USERNAME, REPORTING_TOKEN
1. Openstack related with prefix of `OS_`:

    OS_AUTH_URL, OS_USERNAME, OS_PASSWORD, OS_TENANT_NAME
1. Object store - HCP related with prefix of `OS_HCP_`:

    OS_HCP_ID, OS_HCP_SECRET, OS_HCP_URL

## Run command `ersa-kr`
The supported Python version is 2. Most common usages:

`bin/ersa-kr --insecure status`

`bin/ersa-kr --insecure archive --topic emu.pbs --partition 7 --namespace archive`