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

from bdgenomics.workflows.adam_pipeline.native import call_adam, _log_runtime

_log = logging.getLogger(__name__)


def call_deca(job,
              deca_path,
              executor_memory,
              driver_memory,
              executor_cores,
              executors,
              input_path,
              output_path,
              spark_home,
              deca_cmd,
              args,
              replication=2,
              delete_input=False):
    
    check_call(["hdfs", "dfs",
                "-rm",
                "-r", "-f", "-skipTrash",
                output_path])
    
    cmd = [deca_path,
           "--master", "yarn",
           "--deploy-mode", "cluster",
           "--conf", "spark.hadoop.dfs.replication=%d" % replication,
           "--packages", "org.apache.parquet:parquet-avro:1.8.2",
           "--num-executors", str(executors),
           "--executor-memory", executor_memory,
           "--executor-cores", str(executor_cores),
           "--driver-memory", driver_memory,
           "--",
           deca_cmd,
           "-I", input_path,
           "-o", output_path]
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


def deca_coverage(job,
                  deca_path,
                  executor_memory,
                  driver_memory,
                  executor_cores,
                  executors,
                  input_path,
                  spark_home,
                  targets_path):

    output_path = input_path.replace(".sorted.adam", ".coverage")

    call_deca(job,
              deca_path,
              executor_memory,
              driver_memory,
              executor_cores,
              executors,
              input_path,
              output_path,
              spark_home,
              "coverage",
              ["-L", targets_path],
              replication=2,
              delete_input=True)


def adam_sort(job,
              adam_path,
              deca_path,
              executor_memory,
              driver_memory,
              executor_cores,
              executors,
              input_path,
              spark_home,
              targets_path):
    
    output_path = input_path.replace(".reads.adam", ".sorted.adam")

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
              ["-sort_reads",
               "-limit_projection"],
              replication=1,
              delete_input=False)

    job.addChildJobFn(deca_coverage,
                      deca_path,
                      executor_memory,
                      driver_memory,
                      executor_cores,
                      executors,
                      output_path,
                      spark_home,
                      targets_path)


def queue_samples(job,
                  samples,
                  adam_path,
                  deca_path,
                  executor_memory,
                  driver_memory,
                  executor_cores,
                  spark_home,
                  targets_path):

    block_size = 128

    for (input_path, size) in samples:

        executors = int(size * (1024 / block_size) / executor_cores) + 1

        job.addChildJobFn(adam_sort,
                          adam_path,
                          deca_path,
                          executor_memory,
                          driver_memory,
                          executor_cores,
                          executors,
                          input_path,
                          spark_home,
                          targets_path)

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--manifest', required=True,
                        help='Manifest file with (sample\tinput path\toutput path) per line')
    parser.add_argument('-c', '--adam-path', required=True,
                        help='Path to adam-submit')
    parser.add_argument('-D', '--deca-path', required=True,
                        help='Path to deca-submit')
    parser.add_argument('-x', '--executor-memory', default='25g',
                        help='Amount of memory per Spark executor.')
    parser.add_argument('-d', '--driver-memory', default='5g',
                        help='Amount of memory on the Spark driver')
    parser.add_argument('-k', '--executor-cores', default=4,
                        help='Amount of cores per Spark executor.')
    parser.add_argument('-S', '--spark-home', required=True,
                        help='Path to SPARK_HOME')
    parser.add_argument('-T', '--targets-path', required=True,
                        help='Path to CNV targets file')

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
                                       args.deca_path,
                                       args.executor_memory,
                                       args.driver_memory,
                                       args.executor_cores,
                                       args.spark_home,
                                       args.targets_path), args)

if __name__ == "__main__":
    main()
