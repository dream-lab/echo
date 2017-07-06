# ECHO

## Introduction

ECHO (**O**rchestration platform for **H**ybrid dataflows across **C**loud and **E**dge) is a platform that operates across diverse distributed resources to enable execution of complex analytical dataflows. It can operate on diverse data models - streams, micro-batches and files, and interface with native runtime engines like TensorFlow and Sorm to execute them. It manages the applcation's lifecycle, and can schedule the dataflow on Edge, Fog and Cloud resources while enabling dynamic task migration between the resources. 

## Dataflows

<!--- Add example dataflows here? wait, there are examples in the thingy itself. -->
There are multiple example dataflows that are present in the input-dags folder.

## Design

The project is split into three executing items. They are

* Resource Directory
* Execution Engine
* Device Deamon

The Device Deamon is deployed on all the local machines that make up the edge execution network of the test bed. The other two components make up the central control entity that is deployed on a remote cloud device.

## Deployment

Build the project using `mvn install`. All the required JARs will be built in their respective target folders.

### Resource Directory

The Resource Directory is present in the *Registry* module. The target folder will contain `HyperCatServer.war` which should be placed in the ROOT directory of a tomcat server. The Dicertory can then be accessed as `<ip_of_tomcat_server>:8080/cat`
	
### Cloud Master 

The Execution Engine is present in the *Master* module. This run a server on port number 8080 which exposes REST APIs to submit, rebalance and stop DAGs.

Run the execution engine as `java -cp <path-to-master-0.1.jar> in.dream_lab.echo.master.ExecutionEngine`

This server acts as the application endpoint for the platform.

<!--- Add things about how to make POST requests. -->

### Device Daemon

<!--- Maybe here add the details about NiFi installation as well.-->
Run the DeviceDaemon.py file as a cron job that executes every minute. 

## Usage

Submit Application to the master by sending a `POST` request to the `<ip_of_cloud_master>:8080/submitDAG` with the DAG representing the application as a request body.



Please refer the paper for detailed information about the platform - https://arxiv.org/abs/1707.00889
