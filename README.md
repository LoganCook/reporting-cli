# reporting-cli [![Build Status](https://travis-ci.org/eResearchSA/reporting-cli.svg)](https://travis-ci.org/eResearchSA/reporting-cli)
Kafka/Reporting CLI written in Python 2, it has modules to:

1. get/put content from/to Kafka
1. act as producers to generate content
1. archive content to S3 like object stores

## Library dependencies

For Ubuntu, run this command to install them:

`sudo apt-get install python-dev liblzma-dev`

## Runtime environment variables
1. Kafka related which are defined with prefix of `REPORTING_`:
    * `REPORTING_SERVER`
    * `REPORTING_USERNAME`
    * `REPORTING_TOKEN`
1. Openstack related with prefix of `OS_`:
    * `OS_AUTH_URL`
    * `OS_USERNAME`
    * `OS_PASSWORD`
    * `OS_TENANT_NAME`
1. Object store - AWS related with prefix of `OS_AWS_`:
    * `OS_AWS_ID`
    * `OS_AWS_SECRET`
    * `OS_AWS_URL`

## `ersa-kr` command examples
* Counts of the contents in Kafka:

  `bin/ersa-kr --insecure status`
* Archive content from Kafka to object store:

  `bin/ersa-kr --insecure archive --topic emu.pbs --partition 7 --namespace archive`
* Archive content from a specific Kafka offset instead of automatically obtaining it from previously archived objects in storage:

  `ersa-kr --insecure archive --topic storage.xfs --partition 7 --prefix 20181016-112448 --namespace archive --offset 1554568`
