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


def call_avocado(job,
                 avocado_path,
                 executor_memory,
                 driver_memory,
                 executor_cores,
                 executors,
                 input_path,
                 output_path,
                 spark_home,
                 avocado_cmd,
                 args,
                 replication=1,
                 delete_input=False):
    
    check_call(["hdfs", "dfs",
                "-rm",
                "-r", "-f", "-skipTrash",
                output_path])
    
    cmd = [avocado_path,
           "--master", "yarn",
           "--deploy-mode", "cluster",
           "--conf", "spark.hadoop.dfs.replication=%d" % replication,
           "--packages", "org.apache.parquet:parquet-avro:1.8.2",
           "--num-executors", str(executors),
           "--executor-memory", executor_memory,
           "--executor-cores", str(executor_cores),
           "--driver-memory", driver_memory,
           "--",
           avocado_cmd,
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


def avocado_discover(job,
                     avocado_path,
                     executor_memory,
                     driver_memory,
                     executor_cores,
                     executors,
                     input_path,
                     spark_home):

    output_path = input_path.replace(".reads.adam", ".candidate_variants.adam")

    call_avocado(job,
                 avocado_path,
                 executor_memory,
                 driver_memory,
                 executor_cores,
                 executors,
                 input_path,
                 output_path,
                 spark_home,
                 "discover",
                 ["-min_phred_to_discover_variant", "15",
                  "-min_observations_to_discover_variant", "3"],
                 replication=2)


def queue_samples(job,
                  samples,
                  avocado_path,
                  executor_memory,
                  driver_memory,
                  executor_cores,
                  spark_home):

    block_size = 128

    for (input_path, size) in samples:

        executors = int(size * (1024 / block_size) / executor_cores) + 1

        job.addChildJobFn(avocado_discover,
                          avocado_path,
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
    parser.add_argument('-c', '--avocado-path', required=True,
                        help='Path to avocado-submit')
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
                                       args.avocado_path,
                                       args.executor_memory,
                                       args.driver_memory,
                                       int(args.executor_cores),
                                       args.spark_home), args)

if __name__ == "__main__":
    main()
