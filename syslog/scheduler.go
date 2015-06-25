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

package syslog

import (
	"fmt"
	"log"

	"strconv"
	"strings"
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	"github.com/mesos/mesos-go/scheduler"
)

type SyslogScheduler struct {
	config           SyslogSchedulerConfig
	runningInstances int32
	tasks            []*mesos.TaskID
}

type SyslogSchedulerConfig struct {
	// Number of CPUs allocated for each created Mesos task.
	CpuPerTask float64

	// Number of RAM allocated for each created Mesos task.
	MemPerTask float64

	// Artifact server host name. Will be used to fetch the executor.
	ArtifactServerHost string

	// Artifact server port.Will be used to fetch the executor.
	ArtifactServerPort int

	// Name of the executor archive file.
	ExecutorArchiveName string

	// Name of the executor binary file contained in the executor archive.
	ExecutorBinaryName string

	// Maximum retries to kill a task.
	KillTaskRetries int

	// Number of task instances to run.
	Instances int

	// Producer config file name.
	ProducerConfig string

	// Topic to produce transformed data to.
	Topic string

	// Flag to respond only after decoding-encoding is done.
	Sync bool

	LogLevel string

	// Mesos master ip:port
	Master string
}

func NewSyslogScheduler(config SyslogSchedulerConfig) *SyslogScheduler {
	scheduler := &SyslogScheduler{}
	scheduler.config = config
	return scheduler
}

// mesos.Scheduler interface method.
// Invoked when the scheduler successfully registers with a Mesos master.
func (ss *SyslogScheduler) Registered(driver scheduler.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Printf("Framework Registered with Master %s\n", masterInfo)
}

// mesos.Scheduler interface method.
// Invoked when the scheduler re-registers with a newly elected Mesos master.
func (ss *SyslogScheduler) Reregistered(driver scheduler.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Printf("Framework Re-Registered with Master %s\n", masterInfo)
}

// mesos.Scheduler interface method.
// Invoked when the scheduler becomes "disconnected" from the master.
func (ss *SyslogScheduler) Disconnected(scheduler.SchedulerDriver) {
	log.Println("Disconnected")
}

// mesos.Scheduler interface method.
// Invoked when resources have been offered to ss framework.
func (ss *SyslogScheduler) ResourceOffers(driver scheduler.SchedulerDriver, offers []*mesos.Offer) {
	log.Println("Received offers")

	if int(ss.runningInstances) > ss.config.Instances {
		toKill := int(ss.runningInstances) - ss.config.Instances
		for i := 0; i < toKill; i++ {
			driver.KillTask(ss.tasks[i])
		}

		ss.tasks = ss.tasks[toKill:]
	}

	offersAndTasks := make(map[*mesos.Offer][]*mesos.TaskInfo)
	for _, offer := range offers {
		cpus := getScalarResources(offer, "cpus")
		mems := getScalarResources(offer, "mem")
		ports := getRangeResources(offer, "ports")

		remainingCpus := cpus
		remainingMems := mems

		var tasks []*mesos.TaskInfo
		for int(ss.getRunningInstances()) < ss.config.Instances && ss.config.CpuPerTask <= remainingCpus && ss.config.MemPerTask <= remainingMems && len(ports) > 0 {
			tcpPort := ss.takePort(&ports)
			udpPort := ss.takePort(&ports)
			taskPort := &mesos.Value_Range{Begin: tcpPort, End: udpPort}
			taskId := &mesos.TaskID{
				Value: proto.String(fmt.Sprintf("syslog-%s-%d:%d", *offer.Hostname, *tcpPort, *udpPort)),
			}

			task := &mesos.TaskInfo{
				Name:     proto.String(taskId.GetValue()),
				TaskId:   taskId,
				SlaveId:  offer.SlaveId,
				Executor: ss.createExecutor(ss.getRunningInstances(), *tcpPort, *udpPort),
				Resources: []*mesos.Resource{
					util.NewScalarResource("cpus", float64(ss.config.CpuPerTask)),
					util.NewScalarResource("mem", float64(ss.config.MemPerTask)),
					util.NewRangesResource("ports", []*mesos.Value_Range{taskPort}),
				},
			}
			log.Printf("Prepared task: %s with offer %s for launch. Ports: %s\n", task.GetName(), offer.Id.GetValue(), taskPort)

			tasks = append(tasks, task)
			remainingCpus -= ss.config.CpuPerTask
			remainingMems -= ss.config.MemPerTask
			ports = ports[1:]

			ss.tasks = append(ss.tasks, taskId)
			ss.incRunningInstances()
		}
		log.Printf("Launching %d tasks for offer %s\n", len(tasks), offer.Id.GetValue())
		offersAndTasks[offer] = tasks
	}

	unlaunchedTasks := ss.config.Instances - int(ss.getRunningInstances())
	if unlaunchedTasks > 0 {
		log.Printf("There are still %d tasks to be launched and no more resources are available.", unlaunchedTasks)
	}

	for _, offer := range offers {
		tasks := offersAndTasks[offer]
		driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
	}
}

// mesos.Scheduler interface method.
// Invoked when the status of a task has changed.
func (ss *SyslogScheduler) StatusUpdate(driver scheduler.SchedulerDriver, status *mesos.TaskStatus) {
	log.Printf("Status update: task %s is in state %s\n", status.TaskId.GetValue(), status.State.Enum().String())

	if status.GetState() == mesos.TaskState_TASK_LOST || status.GetState() == mesos.TaskState_TASK_FAILED || status.GetState() == mesos.TaskState_TASK_FINISHED {
		ss.removeTask(status.GetTaskId())
		ss.decRunningInstances()
	}
}

// mesos.Scheduler interface method.
// Invoked when an offer is no longer valid.
func (ss *SyslogScheduler) OfferRescinded(scheduler.SchedulerDriver, *mesos.OfferID) {}

// mesos.Scheduler interface method.
// Invoked when an executor sends a message.
func (ss *SyslogScheduler) FrameworkMessage(scheduler.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, string) {
}

// mesos.Scheduler interface method.
// Invoked when a slave has been determined unreachable
func (ss *SyslogScheduler) SlaveLost(scheduler.SchedulerDriver, *mesos.SlaveID) {}

// mesos.Scheduler interface method.
// Invoked when an executor has exited/terminated.
func (ss *SyslogScheduler) ExecutorLost(scheduler.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, int) {
}

// mesos.Scheduler interface method.
// Invoked when there is an unrecoverable error in the scheduler or scheduler driver.
func (ss *SyslogScheduler) Error(driver scheduler.SchedulerDriver, err string) {
	log.Printf("Scheduler received error: %s\n", err)
}

func getScalarResources(offer *mesos.Offer, resourceName string) float64 {
	resources := 0.0
	filteredResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
		return res.GetName() == resourceName
	})
	for _, res := range filteredResources {
		resources += res.GetScalar().GetValue()
	}
	return resources
}

func getRangeResources(offer *mesos.Offer, resourceName string) []*mesos.Value_Range {
	resources := make([]*mesos.Value_Range, 0)
	filteredResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
		return res.GetName() == resourceName
	})
	for _, res := range filteredResources {
		resources = append(resources, res.GetRanges().GetRange()...)
	}
	return resources
}

func (ss *SyslogScheduler) getRunningInstances() int32 {
	return atomic.LoadInt32(&ss.runningInstances)
}

func (ss *SyslogScheduler) incRunningInstances() {
	atomic.AddInt32(&ss.runningInstances, 1)
}

func (ss *SyslogScheduler) decRunningInstances() {
	atomic.AddInt32(&ss.runningInstances, -1)
}

func param(key string, value string) string {
	return fmt.Sprintf("--%s %s", key, value)
}

func (ss *SyslogScheduler) createExecutor(instanceId int32, tcpPort uint64, udpPort uint64) *mesos.ExecutorInfo {
	log.Println("Creating executor")
	path := strings.Split(ss.config.ExecutorArchiveName, "/")

	brokers := getBrokers(ss.config.Master)

	params := []string{param("log.level", ss.config.LogLevel),
		param("producer.config", ss.config.ProducerConfig),
		param("num.producers", "1"),
		param("topic", ss.config.Topic),
		param("tcp.port", strconv.FormatUint(tcpPort, 10)),
		param("udp.port", strconv.FormatUint(udpPort, 10)),
		//		param("max.procs", strconv.FormatFloat(ss.config.CpuPerTask, 'f', 2, 64)),
		param("broker.list", strings.Join(brokers, ",")),
	}

	paramString := strings.Join(params, " ")
	log.Printf("Param string: %s\n", paramString)

	return &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID(fmt.Sprintf("syslog-%d", instanceId)),
		Name:       proto.String("Syslog Executor"),
		Source:     proto.String("cisco"),
		Command: &mesos.CommandInfo{
			Value: proto.String(fmt.Sprintf("./%s %s", ss.config.ExecutorBinaryName, paramString)),
			Uris: []*mesos.CommandInfo_URI{&mesos.CommandInfo_URI{
				Value:   proto.String(fmt.Sprintf("http://%s:%d/resource/%s", ss.config.ArtifactServerHost, ss.config.ArtifactServerPort, path[len(path)-1])),
				Extract: proto.Bool(true),
			}, &mesos.CommandInfo_URI{
				Value: proto.String(fmt.Sprintf("http://%s:%d/resource/%s", ss.config.ArtifactServerHost, ss.config.ArtifactServerPort, ss.config.ProducerConfig)),
			}},
		},
	}
}

func (ss *SyslogScheduler) takePort(ports *[]*mesos.Value_Range) *uint64 {
	port := (*ports)[0].Begin
	portRange := (*ports)[0]
	portRange.Begin = proto.Uint64((*portRange.Begin) + 1)

	if *portRange.Begin > *portRange.End {
		*ports = (*ports)[1:]
	} else {
		(*ports)[0] = portRange
	}

	return port
}

func (ss *SyslogScheduler) removeTask(id *mesos.TaskID) {
	for i, task := range ss.tasks {
		if *task.Value == *id.Value {
			ss.tasks = append(ss.tasks[:i], ss.tasks[i+1:]...)
		}
	}
}

// Gracefully shuts down all running tasks.
func (ss *SyslogScheduler) Shutdown(driver scheduler.SchedulerDriver) {
	fmt.Println("Shutting down scheduler.")

	for _, taskId := range ss.tasks {
		if err := ss.tryKillTask(driver, taskId); err != nil {
			fmt.Printf("Failed to kill task %s\n", taskId.GetValue())
		}
	}
}

func (ss *SyslogScheduler) tryKillTask(driver scheduler.SchedulerDriver, taskId *mesos.TaskID) error {
	fmt.Printf("Trying to kill task %s\n", taskId.GetValue())

	var err error
	for i := 0; i <= ss.config.KillTaskRetries; i++ {
		if _, err = driver.KillTask(taskId); err == nil {
			return nil
		}
	}
	return err
}
