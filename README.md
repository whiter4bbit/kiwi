[![Build Status](https://travis-ci.org/whiter4bbit/kiwi.svg?branch=master)](https://travis-ci.org/whiter4bbit/kiwi)

# Kiwi
Kiwi is message queue inspired by Kafka (http://kafka.apache.org/documentation.html#design). Kafka is fast, 
well-designed message queue with replication support. However it may be overhead for some projects (e.g. during prototyping) to start using Kafka - 
in addition you also need to start Zookeeper (even with single node setup). Also Kafka clients should deal with custom 
binary protocol and handle communication with Zookeeper.

Kiwi exposes http api for producers and consumers which makes easier to integrate it in your project, because all 
modern programming languages have http client in stdlib. 

# Getting started
Project written in scala and uses [sbt build tool](http://scala-sbt.org).

    sbt assembly
    java -jar target/scala-2.11/kiwi.jar

It will start Kiwi with default configuration. Log file is located at `kiwi.log` in current directory by default. 
To test setup you can start `ConsoleProducer` and `ConsoleConsumer` in separate terminal windows:

````
$ sbt 'run-main phi.client.ConsoleProducer'
[info] Loading project definition from /Users/whiter4bbit/programming/scala/persistent-queue/project
[info] Set current project to kiwi (in build file:/Users/whiter4bbit/programming/scala/persistent-queue/)
[info] Running phi.client.ConsoleProducer 
фев 16, 2015 1:33:56 AM com.twitter.finagle.Init$ apply
INFO: Finagle version 6.24.0 (rev=1e06db17ca2de4b85209dd2fbc18e635815e994b) built at 20141212-145739
фев 16, 2015 1:33:56 AM com.twitter.finagle.AsyncInetResolver <init>
INFO: networkaddress.cache.ttl is not set, DNS cache refresh turned off
producer > message-1
OK
producer > message-2
OK
producer > message-3
OK
````

````
$ sbt 'run-main phi.client.ConsoleConsumer'
[info] Loading project definition from /Users/whiter4bbit/programming/scala/persistent-queue/project
[info] Set current project to kiwi (in build file:/Users/whiter4bbit/programming/scala/persistent-queue/)
[info] Running phi.client.ConsoleConsumer 
фев 16, 2015 1:34:56 AM com.twitter.finagle.Init$ apply
INFO: Finagle version 6.24.0 (rev=1e06db17ca2de4b85209dd2fbc18e635815e994b) built at 20141212-145739
фев 16, 2015 1:34:56 AM com.twitter.finagle.AsyncInetResolver <init>
INFO: networkaddress.cache.ttl is not set, DNS cache refresh turned off
message-1
message-2
message-3
````

# HTTP API

## Data format

Currently supported only binary length-separated format for message body. Both producers and consumers should use this 
format in request body.

```
length: 4 bytes
data: length bytes
```

## Producer

`POST /topic-name` - append messages from request body to topic `topic-name`

## Consumer

Kiwi supports two kinds of consumer apis: global consumer and offset consumer. Former provides at-most-once delivery
semantics. Offset consumer provides at-least-once delivery semantics, you will get messages and `X-Offset` header 
will contain next offset, when your messages will be successfully processed this offset should be updated.

### Global consumer

`GET /topic-name/10` - returns immediately at most 10 messages


`GET /topic-name/await/10/5000` - returns after <= 5000ms at most 10 messages

### Offset consumer

`GET /topic-name/consumer/consumer-id/10` - returns immediately at most 10 messages


`GET /topic-name/consumer/consumer-id/await/10/5000` - returns after <= 5000ms at most 10 messages

## Offset

`GET /topic-name/consumer-id/offset` - returns current offset for consumer `consumer-id`

`POST /topic-name/consumer-id/offset/1000` - updates offset for consumer `consumer-id` to 1000


# Configuration

Kiwi uses [Typesafe Confg](https://github.com/typesafehub/config). Default configuration parameters
can be found in project tree: [reference.conf](https://github.com/whiter4bbit/kiwi/blob/master/src/main/resources/reference.conf).
You can overwrite default parameters and specify path to config using `--config argument`:

    java -jar target/scala-2.11/kiwi.jar --config custom.conf

# TODO

* Support both binary/json formats using `Content-type` header to make endpoints "curlable"
* Perform comprehensive benchmarking (current [benchmarks](https://github.com/whiter4bbit/kiwi/tree/master/src/main/scala/client/benchmark) was done localy
* Perform crash-tests

