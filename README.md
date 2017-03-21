# SJ Sflow Demo


## Table of contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Preparation](#preparation)
    * [Providers creation](#providers-creation)
    * [Services creation](#services-creation)
    * [Streams creation](#streams-creation)
    * [Instances creation](#instances-creation)


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

To create services

```bash
$ curl --request POST "http://$address/v1/services" -H 'Content-Type: application/json' --data "@api-json/services/cassandra-sflow-service.json"
$ curl --request POST "http://$address/v1/services" -H 'Content-Type: application/json' --data "@api-json/services/jdbc-sflow-service.json"
$ curl --request POST "http://$address/v1/services" -H 'Content-Type: application/json' --data "@api-json/services/tstream-sflow-service.json"
$ curl --request POST "http://$address/v1/services" -H 'Content-Type: application/json' --data "@api-json/services/zookeeper-sflow-service.json"
```

### Streams creation

To create an output streams of input module
```bash
$ curl --request POST "http://$address/v1/streams" -H 'Content-Type: application/json' --data "@api-json/streams/sflow-avro.json"
$ curl --request POST "http://$address/v1/streams" -H 'Content-Type: application/json' --data "@api-json/streams/sflow-fallback.json"
```

- *sflow-avro* &mdash; stream for correctly parsed sflow records;
- *sflow-fallback* &mdash; stream for incorrect inputs.

To create an output streams of process module

```bash
$ curl --request POST "http://$address/v1/streams" -H 'Content-Type: application/json' --data "@api-json/streams/DstAsData.json"
$ curl --request POST "http://$address/v1/streams" -H 'Content-Type: application/json' --data "@api-json/streams/DstIpData.json"
$ curl --request POST "http://$address/v1/streams" -H 'Content-Type: application/json' --data "@api-json/streams/SrcAsData.json"
$ curl --request POST "http://$address/v1/streams" -H 'Content-Type: application/json' --data "@api-json/streams/SrcDstData.json"
$ curl --request POST "http://$address/v1/streams" -H 'Content-Type: application/json' --data "@api-json/streams/SrcIpData.json"
```

To create an output streams of output module

```bash
$ curl --request POST "http://$address/v1/streams" -H 'Content-Type: application/json' --data "@api-json/streams/DstAsStream.json"
$ curl --request POST "http://$address/v1/streams" -H 'Content-Type: application/json' --data "@api-json/streams/DstIpStream.json"
$ curl --request POST "http://$address/v1/streams" -H 'Content-Type: application/json' --data "@api-json/streams/SrcAsStream.json"
$ curl --request POST "http://$address/v1/streams" -H 'Content-Type: application/json' --data "@api-json/streams/SrcDstStream.json"
$ curl --request POST "http://$address/v1/streams" -H 'Content-Type: application/json' --data "@api-json/streams/SrcIpStream.json"
```


### Instances creation

To create an instance of input module

```bash
$ curl --request POST "http://$address/v1/modules/input-streaming/com.bwsw.input.csv/1.0/instance" -H 'Content-Type: application/json' --data "@api-json/instances/sflow-csv-input.json" 
```

To create an instance of process module

```bash
$ curl --request POST "http://$address/v1/modules/windowed-streaming/sflow-process/1.0/instance" -H 'Content-Type: application/json' --data "@api-json/instances/sflow-process.json" 
```

To create an instances of output module

```bash
$ curl --request POST "http://$address/v1/modules/output-streaming/sflow-output/1.0/instance" -H 'Content-Type: application/json' --data "@api-json/instances/sflow-dst-as-output.json"
$ curl --request POST "http://$address/v1/modules/output-streaming/sflow-output/1.0/instance" -H 'Content-Type: application/json' --data "@api-json/instances/sflow-dst-ip-output.json"
$ curl --request POST "http://$address/v1/modules/output-streaming/sflow-output/1.0/instance" -H 'Content-Type: application/json' --data "@api-json/instances/sflow-src-as-output.json"
$ curl --request POST "http://$address/v1/modules/output-streaming/sflow-output/1.0/instance" -H 'Content-Type: application/json' --data "@api-json/instances/sflow-src-dst-output.json"
$ curl --request POST "http://$address/v1/modules/output-streaming/sflow-output/1.0/instance" -H 'Content-Type: application/json' --data "@api-json/instances/sflow-src-ip-output.json"
```
 
[TODO]: <> (SQL tables creation)
[TODO]: <> (Describe streams)
