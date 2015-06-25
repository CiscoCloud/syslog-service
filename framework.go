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
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"

	"github.com/CiscoCloud/syslog-service/syslog"
	"github.com/golang/protobuf/proto"
	"github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/scheduler"
)

var (
	instances           = flag.Int("instances", 1, "Number of tasks to run.")
	artifactServerHost  = flag.String("artifact.host", "master", "Binding host for artifact server.")
	artifactServerPort  = flag.Int("artifact.port", 6666, "Binding port for artifact server.")
	master              = flag.String("master", "master:5050", "Mesos Master address <ip:port>.")
	executorArchiveName = flag.String("executor.archive", "executor.tar", "Executor archive name. Absolute or relative path are both ok.")
	executorBinaryName  = flag.String("executor.name", "executor", "Executor binary name contained in archive.")
	cpuPerTask          = flag.Float64("cpu.per.task", 0.2, "CPUs per task.")
	memPerTask          = flag.Float64("mem.per.task", 256, "Memory per task.")
	producerConfig      = flag.String("producer.config", "", "Producer config file name.")
	topic               = flag.String("topic", "", "Topic to produce transformed data to.")
	sync                = flag.Bool("sync", false, "Flag to respond only after decoding-encoding is done.")
	logLevel            = flag.String("log.level", "info", "Level of logging.")
)

func parseAndValidateSchedulerArgs() {
	flag.Parse()

	if *producerConfig == "" {
		fmt.Println("producer.config flag is required.")
		os.Exit(1)
	}

	if *topic == "" {
		fmt.Println("topic flag is required.")
		os.Exit(1)
	}
}

func startArtifactServer(config *syslog.SyslogSchedulerConfig) {
	http.HandleFunc("/scale/", func(w http.ResponseWriter, r *http.Request) {
		scaleTokens := strings.Split(r.URL.Path, "/")
		scale, err := strconv.Atoi(scaleTokens[len(scaleTokens)-1])
		if err != nil {
			panic(err)
		}
		config.Instances = scale
	})
	http.HandleFunc(fmt.Sprintf("/resource/"), func(w http.ResponseWriter, r *http.Request) {
		resourceTokens := strings.Split(r.URL.Path, "/")
		resource := resourceTokens[len(resourceTokens)-1]
		fmt.Println("Serving ", resource)
		http.ServeFile(w, r, resource)
	})
	http.ListenAndServe(fmt.Sprintf("%s:%d", *artifactServerHost, *artifactServerPort), nil)
}

func main() {
	parseAndValidateSchedulerArgs()

	ctrlc := make(chan os.Signal, 1)
	signal.Notify(ctrlc, os.Interrupt)

	frameworkInfo := &mesosproto.FrameworkInfo{
		User: proto.String("vagrant"),
		Name: proto.String("Go Syslog Framework"),
	}

	schedulerConfig := &syslog.SyslogSchedulerConfig{}
	schedulerConfig.ArtifactServerHost = *artifactServerHost
	schedulerConfig.ArtifactServerPort = *artifactServerPort
	schedulerConfig.ExecutorArchiveName = *executorArchiveName
	schedulerConfig.ExecutorBinaryName = *executorBinaryName
	schedulerConfig.Instances = *instances
	schedulerConfig.CpuPerTask = *cpuPerTask
	schedulerConfig.MemPerTask = *memPerTask
	schedulerConfig.ProducerConfig = *producerConfig
	schedulerConfig.Topic = *topic
	schedulerConfig.Sync = *sync
	schedulerConfig.Master = *master
	schedulerConfig.LogLevel = *logLevel

	go startArtifactServer(schedulerConfig)

	syslogScheduler := syslog.NewSyslogScheduler(*schedulerConfig)
	driverConfig := scheduler.DriverConfig{
		Scheduler: syslogScheduler,
		Framework: frameworkInfo,
		Master:    *master,
	}

	driver, err := scheduler.NewMesosSchedulerDriver(driverConfig)
	go func() {
		<-ctrlc
		syslogScheduler.Shutdown(driver)
		driver.Stop(false)
	}()

	if err != nil {
		fmt.Println("Unable to create a SchedulerDriver ", err.Error())
	}

	if stat, err := driver.Run(); err != nil {
		fmt.Println("Framework stopped with status %s and error: %s\n", stat.String(), err.Error())
	}
}
