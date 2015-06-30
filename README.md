# syslog-service
Go based Syslog service that can run within an infrastructure role on Mesos.

Pre-Requisites
==============

- [Golang](http://golang.org/doc/install)
- A standard and working Go workspace setup
- Apache Mesos 0.19 or newer

Build Instructions
=================

- Get the project
```
$ cd $GOPATH/src/
$ mkdir -p github.com/CiscoCloud
$ cd github.com/CiscoCloud
$ git clone https://github.com/CiscoCloud/syslog-service.git
$ cd syslog-service
```

- Build the scheduler and the executor
```
$ go build framework.go
$ go build executor.go
```
- Package the executor (**make sure the built binary has executable permissions before this step!**)
```
$ zip -r executor.zip executor
```
- Place the built framework and executor archive somewhere on Mesos Master node

Running
=======

You will need a running Mesos master and slaves to run. The following commands should be launched on Mesos Master node.

```
$ cd <framework-location>
$ ./framework --master master:5050 --producer.config producer.config --topic syslog
```

*List of available flags:*

```
--artifact.host="master": Binding host for artifact server.
--artifact.port=6666: Binding port for artifact server.
--cpu.per.task=0.2: CPUs per task.
--executor.archive="executor.zip": Executor archive name. Absolute or relative path are both ok.
--executor.name="executor": Executor binary name contained in archive.
--instances=1: Number of tasks to run.
--master="master:5050": Mesos Master address <ip:port>.
--mem.per.task=256: Memory per task.
--producer.config: Producer properties file name.
--sync: Flag to respond only after decoding-encoding is done.
--topic: Topic to produce transformed data to.
--broker.list: If you are not using kafka-mesos, comma-separated list of brokers (ip:port).
--user="vagrant": User to run executor.
--log.level="info": Set logging level.
```
