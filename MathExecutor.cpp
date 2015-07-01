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

#include <iostream>
#include <string>
#include <vector>
#include <assert.h>

#include <stout/lambda.hpp>
#include <stout/numify.hpp>
#include <stout/stringify.hpp>
#include <mesos/executor.hpp>
#include "constants.hpp"

using namespace mesos;

using std::cout;
using std::endl;
using std::string;

// Returns true if the digits in 'a' and 'b' are equivalent
// Zeros are ignored; i.e. 1 and 100 have are equivalent
static bool compareDigits(size_t a, size_t b)
{
  int digits[10] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  while (a > 0) {
    digits[a % 10]++;
    a /= 10;
  }
  while (b > 0) {
    digits[b % 10]--;
    b /= 10;
  }
  for (size_t i = 1; i < 10; i++) {
    if (digits[i] != 0) {
      return false;
    }
  }
  return true;
}

static void runTask(ExecutorDriver* driver, const TaskInfo& task)
{
  size_t number = 0;
  size_t incNum = 0;

  // Extract task specifications from TaskInfo
  const Labels &labels = task.labels();
  for (size_t i = 0; i < labels.labels_size(); i++) {
    const Label &label = labels.labels(i);

    if (label.key().compare(LABEL_KEY_START_NUM) == 0) {
      number = numify<size_t>(label.value()).get();
    } else if (label.key().compare(LABEL_KEY_INC_NUM) == 0) {
      incNum = numify<size_t>(label.value()).get();
    } else {
      cout << "Unknown label key: " << label.key() << endl;
    }
  }

  // Search for a solution to problem #52
  size_t statusUpdate = 0;
  while (true) {
    bool is2 = compareDigits(number, number * 2);
    bool is3 = compareDigits(number, number * 3);
    bool is4 = compareDigits(number, number * 4);
    bool is5 = compareDigits(number, number * 5);
    bool is6 = compareDigits(number, number * 6);
    if (is2 && is3 && is4 && is5 && is6) {
      break;
    }

    number += incNum;

    // Send a status update every 1000 cycles
    statusUpdate = (statusUpdate + 1) % 1000;
    if (statusUpdate == 0) {
      TaskStatus status;
      status.mutable_task_id()->MergeFrom(task.task_id());
      status.set_state(TASK_RUNNING);
      status.set_message(stringify<size_t>(number));
      driver->sendStatusUpdate(status);
    }
  }

  TaskStatus status;
  status.mutable_task_id()->MergeFrom(task.task_id());
  status.set_state(TASK_FINISHED);
  status.set_message(stringify<size_t>(number));
  driver->sendStatusUpdate(status);
}


void* start(void* arg)
{
  lambda::function<void(void)>* thunk = (lambda::function<void(void)>*) arg;
  (*thunk)();
  delete thunk;
  return NULL;
}

class MathExecutor : public Executor
{
  public:
    virtual ~MathExecutor() {}

    virtual void registered(ExecutorDriver* driver,
                            const ExecutorInfo& executorInfo,
                            const FrameworkInfo& frameworkInfo,
                            const SlaveInfo& slaveInfo)
    {
      cout << "Registered MathExecutor on " << slaveInfo.hostname() << endl;
    }

    virtual void reregistered(ExecutorDriver* driver,
                              const SlaveInfo& slaveInfo)
    {
      cout << "Re-registered MathExecutor on " << slaveInfo.hostname() << endl;
    }

    virtual void disconnected(ExecutorDriver* driver) {}


    virtual void launchTask(ExecutorDriver* driver, const TaskInfo& task)
    {
      cout << "Starting task " << task.task_id().value() << endl;

      lambda::function<void(void)>* thunk =
        new lambda::function<void(void)>(lambda::bind(&runTask, driver, task));

      pthread_t pthread;
      if (pthread_create(&pthread, NULL, &start, thunk) != 0) {
        TaskStatus status;
        status.mutable_task_id()->MergeFrom(task.task_id());
        status.set_state(TASK_FAILED);

        driver->sendStatusUpdate(status);
      } else {
        pthread_detach(pthread);

        TaskStatus status;
        status.mutable_task_id()->MergeFrom(task.task_id());
        status.set_state(TASK_RUNNING);

        driver->sendStatusUpdate(status);
      }
    }

  virtual void killTask(ExecutorDriver* driver, const TaskID& taskId) {}
  virtual void frameworkMessage(ExecutorDriver* driver, const string& data) {}
  virtual void shutdown(ExecutorDriver* driver) {}
  virtual void error(ExecutorDriver* driver, const string& message) {}
};


int main(int argc, char** argv)
{
  MathExecutor executor;
  MesosExecutorDriver driver(&executor);
  return driver.run() == DRIVER_STOPPED ? 0 : 1;
}
