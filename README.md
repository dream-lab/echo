# ECHO

## Introduction

ECHO (**O**rchestration platform for **H**ybrid dataflows across **C**loud and **E**dge) is a platform that operates across diverse distributed resources to enable execution of complex analytical dataflows. It can operate on diverse data models - streams, micro-batches and files, and interface with native runtime engines like TensorFlow and Storm to execute them. It manages the applcation's lifecycle, and can schedule the dataflow on Edge, Fog and Cloud resources while enabling dynamic task migration between the resources. 

## Dataflows

<!--- Add example dataflows here? wait, there are examples in the thingy itself. -->
There are multiple example dataflows that are present in the input-dags folder.

## Design

The Framework consists of mainly two modules

* **ECHO Master Service** - This is the central command module that manages all the resources and the applications that make up the framework. It is the point of contact for any user wishing to deploy a new Application onto the platform, or to register a new edge device into the platform

* **ECHO Platform Service** - This is the module that runs on every individual edge device. It maintains a link with the Master Service and executes the control signals received from the master.

### Resource Directory

The Resource Directory is present in the *Registry* module. The target folder will contain `HyperCatServer.war` which should be placed in the ROOT directory of a tomcat server. The Dicertory can then be accessed as `<ip_of_tomcat_server>:8080/cat`
	
### MQTT Broker

The platform uses MQTT messages to transmit control signals between the *Master Service* and the *Platform Service*.


## Usage

Submit Application to the master by sending a `POST` request to the `<ip_of_cloud_master>:8099/DAG` with the DAG representing the application as a request body.

Please refer the paper for detailed information about the platform - https://arxiv.org/abs/1707.00889
