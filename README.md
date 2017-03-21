# SJ Sflow Demo


## Table of contents

- [Prerequisites](#Prerequisites)
- [Installation](#Installation)


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


