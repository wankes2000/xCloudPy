# xCloudPy
[![Build Status](https://travis-ci.org/wankes2000/xCloudPy.svg?branch=master)](https://travis-ci.org/wankes2000/xCloudPy) [![Coverage Status](https://coveralls.io/repos/github/wankes2000/xCloudPy/badge.svg)](https://coveralls.io/github/wankes2000/xCloudPy)

## Description

## Tests

In order to run AWS integration tests you must start localstack into your development environment with this command:

```sh
localstack start --docker
```

In order to run Google Cloud integration tests you must start Google Cloud emulator with this docker: 

```sh
docker run -p 8282:8282 google/cloud-sdk gcloud beta emulators datastore start --no-store-on-disk --project=test --host-port 0.0.0.0:8282
```