# ECHO PLATFORM SERVICE

## Introduction

**ECHO Platform Service** runs on every Edge device of the *ECHO Platform*. It maintains a constant link with the central *Master Service* and facilitates the execution of the control signals received from the master.

## Deployment

Before the platform service can be deployed, each Edge device must have Apache NiFi downloaded and placed in `/app`. ECHO has been tested with Apache NiFi 1.2.0. To download and untar Apache NiFi, use `cd /app && wget https://archive.apache.org/dist/nifi/1.2.0/nifi-1.2.0-bin.tar.gz && tar -zxvf nifi-1.2.0-bin.tar.gz`. 

Clone the ECHO Github repository and move the contents of the `echo_platform_service` to the `\app` folder.

Alternatively, the Dockerfile in the ECHO Github repository can be built using the `docker build` command. This container contains all the depenedencies including Apache NiFi and platform service in the right directories.

The platform service can then deployed on every Edge device using `bootstrap.py <device_uuid> <resource_directory_ip:port> <mqtt_client> <resource_update_frequency>`.
