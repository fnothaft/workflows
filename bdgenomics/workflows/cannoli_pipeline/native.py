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


def _log_runtime(start, end, cmd):

    elapsed_time = end - start
    
    hours = int(elapsed_time) / (60 * 60)
    minutes = int(elapsed_time - (60 * 60 * hours)) / 60
    seconds = int(elapsed_time - (60 * 60 * hours) - (60 * minutes)) % 60

    _log.info("%s ran in %dh%dm%ds (%d to %d)" % (cmd,
                                                  hours,
                                                  minutes,
                                                  seconds,
                                                  start,
                                                  end))


def call_cannoli(job,
                 cannoli_path,
                 executor_memory,
                 driver_memory,
                 executor_cores,
                 executors,
                 sample,
                 input_path,
                 output_path,
                 index_path,
                 sequence_dictionary,
                 bwa_path,
                 spark_home):

    check_call(["hdfs", "dfs",
                "-rm",
                "-r", "-f", "-skipTrash",
                output_path])

    cmd = [cannoli_path,
           "--master", "yarn",
           "--deploy-mode", "cluster",
           "--conf", "spark.hadoop.dfs.replication=1",
           "--packages", "org.apache.parquet:parquet-avro:1.8.2",
           "--num-executors", str(executors),
           "--executor-memory", executor_memory,
           "--executor-cores", str(executor_cores),
           "--driver-memory", driver_memory,
           "--", "bwa",
           input_path,
           output_path,
           sample,
           "-index", index_path,
           "-sequence_dictionary", sequence_dictionary,
           "-bwa_path", bwa_path]

    start_time = time.time()

    check_call(cmd, env={'SPARK_HOME': spark_home})

    end_time = time.time()
    _log_runtime(start_time, end_time, sample)


def queue_samples(job,
                  samples,
                  cannoli_path,
                  executor_memory,
                  driver_memory,
                  executor_cores,
                  index_path,
                  sequence_dictionary,
                  bwa_path,
                  spark_home):

    block_size = 128

    for (sample, input_path, output_path, size) in samples:

        executors = int(size * (1024 / block_size) / executor_cores) + 1

        job.addChildJobFn(call_cannoli,
                          cannoli_path,
                          executor_memory,
                          driver_memory,
                          executor_cores,
                          executors,
                          sample,
                          input_path,
                          output_path,
                          index_path,
                          sequence_dictionary,
                          bwa_path,
                          spark_home)

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--manifest', required=True,
                        help='Manifest file with (sample\tinput path\toutput path) per line')
    parser.add_argument('-c', '--cannoli-path', required=True,
                        help='Path to cannoli-submit')
    parser.add_argument('-x', '--executor-memory', default='25g',
                        help='Amount of memory per Spark executor.')
    parser.add_argument('-d', '--driver-memory', default='5g',
                        help='Amount of memory on the Spark driver')
    parser.add_argument('-k', '--executor-cores', default=4,
                        help='Amount of cores per Spark executor.')
    parser.add_argument('-i', '--index', required=True,
                        help='Path to BWA indices')
    parser.add_argument('-s', '--sequence-dictionary', required=True,
                        help='Path to sequence dictionary')
    parser.add_argument('-b', '--bwa', required=True,
                        help='Path to BWA')
    parser.add_argument('-S', '--spark-home', required=True,
                        help='Path to SPARK_HOME')

    Job.Runner.addToilOptions(parser)
    args = parser.parse_args()

    # loop and process samples
    fp = open(args.manifest)
    samples = []
    for line in fp:
        
        line = line.split()
        
        samples.append((line[0], line[1], line[2], float(line[3])))

    Job.Runner.startToil(Job.wrapJobFn(queue_samples,
                                       samples,
                                       args.cannoli_path,
                                       args.executor_memory,
                                       args.driver_memory,
                                       args.executor_cores,
                                       args.index,
                                       args.sequence_dictionary,
                                       args.bwa,
                                       args.spark_home), args)

if __name__ == "__main__":
    main()
