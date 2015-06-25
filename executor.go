/* Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/CiscoCloud/syslog-service/syslog"
	"github.com/Shopify/sarama"
	"github.com/mesos/mesos-go/executor"
	kafka "github.com/stealthly/go_kafka_client"
)

var (
	logLevel       = flag.String("log.level", "info", "Log level for built-in logger.")
	producerConfig = flag.String("producer.config", "", "Path to producer configuration file.")
	numProducers   = flag.Int("num.producers", 1, "Number of producers.")
	queueSize      = flag.Int("queue.size", 10000, "Number of messages that are buffered between the consumer and producer.")
	topic          = flag.String("topic", "", "Topic to produce messages into.")
	tcpPort        = flag.String("tcp.port", "5140", "TCP port to listen for incoming messages.")
	tcpHost        = flag.String("tcp.host", "0.0.0.0", "TCP host to listen for incoming messages.")
	udpPort        = flag.String("udp.port", "5141", "UDP port to listen for incoming messages.")
	udpHost        = flag.String("udp.host", "0.0.0.0", "UDP host to listen for incoming messages.")
	maxProcs       = flag.Int("max.procs", runtime.NumCPU(), "Maximum number of CPUs that can be executing simultaneously.")
	brokerList     = flag.String("broker.list", "", "Broker List to produce messages too.")
	requiredAcks   = flag.Int("required.acks", 1, "Required acks for producer. 0 - no server response. 1 - the server will wait the data is written to the local log. -1 - the server will block until the message is committed by all in sync replicas.")
	acksTimeout    = flag.Int("acks.timeout", 1000, "This provides a maximum time in milliseconds the server can await the receipt of the number of acknowledgements in RequiredAcks.")
)

func parseAndValidateArgs() *kafka.SyslogProducerConfig {
	flag.Parse()

	setLogLevel()
	runtime.GOMAXPROCS(*maxProcs)

	if *brokerList == "" {
		fmt.Println("broker.list is required.")
		os.Exit(1)
	}

	if *topic == "" {
		fmt.Println("Topic is required.")
		os.Exit(1)
	}

	if *queueSize < 0 {
		fmt.Println("Queue size should be equal or greater than 0")
		os.Exit(1)
	}

	config := kafka.NewSyslogProducerConfig()
	conf, err := kafka.ProducerConfigFromFile(*producerConfig)
	useFile := true
	if err != nil {
		//we dont have a producer configuraiton which is ok
		useFile = false
	} else {
		if err = conf.Validate(); err != nil {
			panic(err)
		}
	}

	if useFile {
		config.ProducerConfig = conf
	} else {
		config.ProducerConfig = kafka.DefaultProducerConfig()
		config.ProducerConfig.Acks = *requiredAcks
		config.ProducerConfig.Timeout = time.Duration(*acksTimeout) * time.Millisecond
	}
	config.NumProducers = *numProducers
	config.ChannelSize = *queueSize
	config.Topic = *topic
	config.BrokerList = *brokerList
	config.TCPAddr = fmt.Sprintf("%s:%s", *tcpHost, *tcpPort)
	config.UDPAddr = fmt.Sprintf("%s:%s", *udpHost, *udpPort)
	config.Transformer = stringTransformer

	return config
}

func setLogLevel() {
	var level kafka.LogLevel
	switch strings.ToLower(*logLevel) {
	case "trace":
		level = kafka.TraceLevel
	case "debug":
		level = kafka.DebugLevel
	case "info":
		level = kafka.DebugLevel
	case "warn":
		level = kafka.DebugLevel
	case "error":
		level = kafka.DebugLevel
	case "critical":
		level = kafka.DebugLevel
	default:
		{
			fmt.Printf("Invalid log level: %s\n", *logLevel)
			os.Exit(1)
		}
	}
	kafka.Logger = kafka.NewDefaultLogger(level)
}

func main() {
	config := parseAndValidateArgs()
	producer := kafka.NewSyslogProducer(config)
	driverConfig := executor.DriverConfig{
		Executor: syslog.NewSyslogExecutor(producer),
	}
	driver, err := executor.NewMesosExecutorDriver(driverConfig)
	if err != nil {
		fmt.Println("Unable to create a ExecutorDriver ", err.Error())
	}
	_, err = driver.Start()
	if err != nil {
		fmt.Println("Got error:", err)
		return
	}
	fmt.Println("Executor process has started and running.")
	driver.Join()
}

func stringTransformer(msg *kafka.SyslogMessage, topic string) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(msg.Message)}
}
