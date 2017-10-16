#!/usr/bin/env python2.7
#
# Licensed to Big Data Genomics (BDG) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The BDG licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import logging
from subprocess import check_call
import time

from toil.job import Job


_log = logging.getLogger(__name__)


def _log_runtime(job, start, end, cmd):

    elapsed_time = end - start
    
    hours = int(elapsed_time) / (60 * 60)
    minutes = int(elapsed_time - (60 * 60 * hours)) / 60
    seconds = int(elapsed_time - (60 * 60 * hours) - (60 * minutes)) % 60

    job.fileStore.logToMaster("%s ran in %dh%dm%ds (%d to %d)" % (cmd,
                                                                  hours,
                                                                  minutes,
                                                                  seconds,
                                                                  start,
                                                                  end))


def call_adam(job,
              adam_path,
              executor_memory,
              driver_memory,
              executor_cores,
              executors,
              input_path,
              output_path,
              spark_home,
              adam_cmd,
              args,
              replication=1,
              delete_input=False):
    
    check_call(["hdfs", "dfs",
                "-rm",
                "-r", "-f", "-skipTrash",
                output_path])
    
    cmd = [adam_path,
           "--master", "yarn",
           "--deploy-mode", "cluster",
           "--conf", "spark.hadoop.dfs.replication=%d" % replication,
           "--packages", "org.apache.parquet:parquet-avro:1.8.2",
           "--num-executors", str(executors),
           "--executor-memory", executor_memory,
           "--executor-cores", str(executor_cores),
           "--driver-memory", driver_memory,
           "--",
           adam_cmd,
           input_path,
           output_path]
    cmd.extend(args)

    start_time = time.time()

    check_call(cmd, env={'SPARK_HOME': spark_home})

    end_time = time.time()
    _log_runtime(job, start_time, end_time, input_path)

    if delete_input:

        check_call(["hdfs", "dfs",
                    "-rm",
                    "-r", "-f", "-skipTrash",
                    input_path])


def adam_realign(job,
                 adam_path,
                 executor_memory,
                 driver_memory,
                 executor_cores,
                 executors,
                 input_path,
                 spark_home):

    output_path = input_path.replace("alignments",
                                     "analysis").replace(".adam", ".reads.adam")

    call_adam(job,
              adam_path,
              executor_memory,
              driver_memory,
              executor_cores,
              executors,
              input_path,
              output_path,
              spark_home,
              "transformAlignments",
              ["-realign_indels",
               "-aligned_read_predicate",
               "-limit_projection",
               "-log_odds_threshold", "0.5"],
              replication=2)


def queue_samples(job,
                  samples,
                  adam_path,
                  executor_memory,
                  driver_memory,
                  executor_cores,
                  spark_home):

    block_size = 128

    for (input_path, size) in samples:

        executors = int(size * (1024 / block_size) / executor_cores) + 1

        job.addChildJobFn(adam_realign,
                          adam_path,
                          executor_memory,
                          driver_memory,
                          executor_cores,
                          executors,
                          input_path,
                          spark_home)

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--manifest', required=True,
                        help='Manifest file with (sample\tinput path\toutput path) per line')
    parser.add_argument('-c', '--adam-path', required=True,
                        help='Path to cannoli-submit')
    parser.add_argument('-x', '--executor-memory', default='25g',
                        help='Amount of memory per Spark executor.')
    parser.add_argument('-d', '--driver-memory', default='5g',
                        help='Amount of memory on the Spark driver')
    parser.add_argument('-k', '--executor-cores', default=4,
                        help='Amount of cores per Spark executor.')
    parser.add_argument('-S', '--spark-home', required=True,
                        help='Path to SPARK_HOME')

    Job.Runner.addToilOptions(parser)
    args = parser.parse_args()

    # loop and process samples
    fp = open(args.manifest)
    samples = []
    for line in fp:
        
        line = line.split()
        
        samples.append((line[0], float(line[1])))

    Job.Runner.startToil(Job.wrapJobFn(queue_samples,
                                       samples,
                                       args.adam_path,
                                       args.executor_memory,
                                       args.driver_memory,
                                       args.executor_cores,
                                       args.spark_home), args)

if __name__ == "__main__":
    main()
