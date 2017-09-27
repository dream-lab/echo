# ECHO MASTER SERVICE

## Introduction

The Echo Master Service is the central authority of the Echo Framework that manages the resources that make up the framework and the applications that are running on it.

## Deployment

Build the service by running `mvn install` on the main directory and then start the server using `java -jar modules/master/target/master-0.1.jar server echo-configuration.yml`

The *echo-configuration.yml* file contains the details about the Resource Directory and the MQTT broker that is used by the service. They must be updated to the appropriate values before starting the server.

## Usage

The server starts up on port *8099*. Applications can be submitted to the server by sending a **POST** to `<ip-of-server>:8099/DAG` with the *JSON* representing the DAG as the body of the HTTP Message. The server returns a response JSON to the client which includes the *UUID* of the application.

Applications can be stopped by sending a **POST** to `<ip-of-server>:8099/DAG/stop?uuid=<uuid-of-application>`.
