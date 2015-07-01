/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <libgen.h>
#include <stdlib.h>
#include <limits.h>
#include <signal.h>
#include <unistd.h>
#include <iostream>
#include <string>
#include <vector>
#include <mesos/resources.hpp>
#include <mesos/scheduler.hpp>
#include <stout/stringify.hpp>
#include "constants.hpp"

using namespace mesos;

using std::cout;
using std::endl;
using std::string;
using std::vector;
using std::queue;
using std::map;

using mesos::Resources;

const float CPUS_PER_TASK = 0.4;
const int32_t MEM_PER_TASK = 1;

MesosSchedulerDriver* schedulerDriver;

static void SIGINTHandler();

class SomeScheduler : public Scheduler
{
public:
  SomeScheduler(const ExecutorInfo& _bruteForcer) 
    : bruteForcer(_bruteForcer),
      tasksLaunched(0),
      tasksFinished(0), 
      answer(0) {}

  virtual ~SomeScheduler() {}

  virtual void registered(SchedulerDriver*, const FrameworkID&, const MasterInfo&)
  {
    cout << "Registered!" << endl;
  }

  virtual void reregistered(SchedulerDriver*, const MasterInfo& masterInfo) {}

  virtual void disconnected(SchedulerDriver* driver) {}

  virtual void resourceOffers(SchedulerDriver* driver,
      const vector<Offer>& offers)
  {
    if (tasksLaunched) return;

    static Resources TASK_RESOURCES = Resources::parse(
        "cpus:" + stringify<float>(CPUS_PER_TASK) +
        ";mem:" + stringify<size_t>(MEM_PER_TASK)).get();

    // Determine the maximum number of tasks
    size_t maxTasks = 0;
    for (size_t i = 0; i < offers.size(); i++) {
      const Offer& offer = offers[i];
      Resources remaining = offer.resources();

      while (remaining.flatten().contains(TASK_RESOURCES)) {
        maxTasks++;
        remaining -= TASK_RESOURCES;
      }
    }

    size_t counter = 0;
    for (size_t i = 0; i < offers.size(); i++) {
      const Offer& offer = offers[i];
      Resources remaining = offer.resources();

      // Spawn a task per number
      vector<TaskInfo> tasks;
      while (remaining.flatten().contains(TASK_RESOURCES)) {
        counter++;
        remaining -= TASK_RESOURCES;

        string bfID = "BF" + stringify<size_t>(counter);

        TaskInfo task;
        task.set_name("BruteForcer " + bfID);
        task.mutable_task_id()->set_value(bfID);
        task.mutable_slave_id()->MergeFrom(offer.slave_id());
        task.mutable_executor()->MergeFrom(bruteForcer);
        task.mutable_resources()->MergeFrom(TASK_RESOURCES);

        Labels *labels = task.mutable_labels();
        Label *label = labels->add_labels();
        label->set_key(LABEL_KEY_START_NUM);
        label->set_value(stringify<size_t>(counter));
        
        label = labels->add_labels();
        label->set_key(LABEL_KEY_INC_NUM);
        label->set_value(stringify<size_t>(maxTasks));

        tasksLaunched++;
        tasks.push_back(task);
      }

      driver->launchTasks(offer.id(), tasks);
    }
  }

  virtual void offerRescinded(SchedulerDriver* driver, const OfferID& offerId) {}

  virtual void statusUpdate(SchedulerDriver* driver, const TaskStatus& status)
  {
    if (status.state() == TASK_FINISHED) {
      if (status.has_message()) {
        size_t number = numify<size_t>(status.message()).get();
        cout << "Task " << status.task_id().value() << " finished with answer = " << number << endl;

        // Compare answers
        if (answer == 0 || number < answer) {
          answer = number;
        }
      } else {
        cout << "Task " << status.task_id().value() << " finished " << endl;
      }
      
      tasksFinished++;
    }

    // if (status.state() == TASK_RUNNING && status.has_message()) {
    //   size_t number = numify<size_t>(status.message()).get();
    //   if (answer != 0 && number > answer) {
    //     cout << "Killing task " << status.task_id().value() << endl;
    //     driver->killTask(status.task_id());
    //   }
    // }

    if (tasksFinished == tasksLaunched) {
      driver->stop();
    }
  }

  virtual void frameworkMessage(SchedulerDriver* driver,
      const ExecutorID& executorId,
      const SlaveID& slaveId,
      const string& data)
  {
    cout << data << endl;
  }

  virtual void slaveLost(SchedulerDriver* driver, const SlaveID& sid) {}

  virtual void executorLost(SchedulerDriver* driver,
      const ExecutorID& executorID,
      const SlaveID& slaveID,
      int status) {}

  virtual void error(SchedulerDriver* driver, const string& message)
  {
    cout << message << endl;
  }

private:
  const ExecutorInfo bruteForcer;
  size_t tasksLaunched;
  size_t tasksFinished;
  size_t answer;
};

static void SIGINTHandler(int signum)
{
  if (schedulerDriver != NULL) {
    schedulerDriver->stop();
  }
  delete schedulerDriver;
  exit(0);
}

#define shift argc--,argv++
int main(int argc, char** argv)
{
  string master;
  shift;
  while (true) {
    string s = argc>0 ? argv[0] : "--help";
    if (argc > 1 && s == "--master") {
      master = argv[1];
      shift; shift;
    } else {
      break;
    }
  }

  if (master.length() == 0) {
    printf("Usage: SomeScheduler --master <ip>:<port>\n");
    exit(1);
  }

  // Find this executable's directory to locate executor.
  string path = realpath(dirname(argv[0]), NULL);
  string mathURI = path + "/MathExecutor";
  cout << mathURI << endl;

  ExecutorInfo mathee;
  mathee.mutable_executor_id()->set_value("BruteForcer");
  mathee.mutable_command()->set_value(mathURI);
  mathee.set_name("Math Executor (C++)");
  mathee.set_source("cpp");

  SomeScheduler scheduler(mathee);

  FrameworkInfo framework;
  framework.set_user(""); // Have Mesos fill in the current user.
  framework.set_name("Some Framework (C++)");
  framework.set_principal("something-cpp");

  // Set up the signal handler for SIGINT for clean shutdown.
  struct sigaction action;
  action.sa_handler = SIGINTHandler;
  sigemptyset(&action.sa_mask);
  action.sa_flags = 0;
  sigaction(SIGINT, &action, NULL);

  schedulerDriver = new MesosSchedulerDriver(&scheduler, framework, master);

  int status = schedulerDriver->run() == DRIVER_STOPPED ? 0 : 1;

  // Ensure that the driver process terminates.
  schedulerDriver->stop();

  delete schedulerDriver;
  return status;
}
