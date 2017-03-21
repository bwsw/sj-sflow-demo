# SJ Sflow Demo


## Table of contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Preparation](#preparation)
    * [Providers creation](#providers-creation)
    * [Services creation](#services-creation)
    * [Streams creation](#streams-creation)


## Prerequisites

- Running [SJ Platform](https://github.com/bwsw/sj-platform) with uploaded CSV Input module.
- [SBT](http://www.scala-sbt.org/)


## Installation

To build and upload all modules of sflow demo

```bash
$ git clone https://github.com/bwsw/sj-sflow-demo.git
$ cd sj-sflow-demo
$ sbt assembly
$ address=<host>:<port>
$ curl --form jar=@sflow-process/target/scala-2.12/sflow-process-1.0.jar http://$address/v1/modules
$ curl --form jar=@sflow-output/target/scala-2.12/sflow-output-1.0.jar http://$address/v1/modules
```

- *\<host\>:\<port\>* &mdash; SJ Rest host and port.


## Preparation

### Providers creation

Before creation a providers you should replace next placeholders in [api-json/providers](api-json/providers):
*\<login\>*, *\<password\>*, *\<host\>* and *\<port\>*. Remove *"login"* and *"password"* fields if you not need 
authentication to appropriate server. 

To create providers

```bash
$ curl --request POST "http://$address/v1/providers" -H 'Content-Type: application/json' --data "@api-json/providers/cassandra-sflow-provider.json" 
$ curl --request POST "http://$address/v1/providers" -H 'Content-Type: application/json' --data "@api-json/providers/jdbc-sflow-provider.json" 
$ curl --request POST "http://$address/v1/providers" -H 'Content-Type: application/json' --data "@api-json/providers/zookeeper-sflow-provider.json" 
```


### Services creation

### Streams creation
