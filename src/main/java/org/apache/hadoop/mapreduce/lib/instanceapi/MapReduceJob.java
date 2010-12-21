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

package org.apache.hadoop.mapreduce.lib.instanceapi;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.SerializableCombinerDelegator;
import org.apache.hadoop.mapreduce.SerializableMapperDelegator;
import org.apache.hadoop.mapreduce.SerializableReducerDelegator;

public class MapReduceJob<K1, V1, K2, V2, K3, V3> {

  private Job job;
  
  public static <K1, V1, K2, V2, K3, V3> MapReduceJob<K1, V1, K2, V2, K3, V3>
      newJob() throws IOException {
    return new MapReduceJob<K1, V1, K2, V2, K3, V3>();
  }
  
  public static <K1, V1, K2, V2, K3, V3> MapReduceJob<K1, V1, K2, V2, K3, V3>
      newJob(Job job) throws IOException {
    return new MapReduceJob<K1, V1, K2, V2, K3, V3>(job);
  }
  
  MapReduceJob() throws IOException {
    this(new Job());
  }
  
  MapReduceJob(Job job) throws IOException {
    this.job = job;
  }
  
  public void setInputFormat(InputFormat<K1, V1> inputFormat) throws IOException {
    if (inputFormat instanceof Serializable) {
      SerializableInputFormatDelegator.setDelegate(job, inputFormat);
    } else {
      job.setInputFormatClass(inputFormat.getClass());
    }
  }  
  
  public void setMapper(Mapper<K1, V1, K2, V2> mapper) throws IOException {
    if (mapper instanceof Serializable) {
      SerializableMapperDelegator.setDelegate(job, mapper);
    } else {
      job.setMapperClass(mapper.getClass());
    }
  }

  public void setPartitioner(Partitioner<K2, V2> partitioner) throws IOException {
    if (partitioner instanceof Serializable) {
      SerializablePartitionerDelegator.setDelegate(job, partitioner);
    } else {
      job.setPartitionerClass(partitioner.getClass());
    }
  }
  
  public void setSortComparator(RawComparator<K2> sortComparator) throws IOException {
    if (sortComparator instanceof Serializable) {
      SerializableSortComparatorDelegator.setDelegate(job, sortComparator);
    } else {
      job.setSortComparatorClass(sortComparator.getClass());
    }
  }

  public void setGroupingComparator(RawComparator<V2> groupingComparator) throws IOException {
    if (groupingComparator instanceof Serializable) {
      SerializableGroupingComparatorDelegator.setDelegate(job, groupingComparator);
    } else {
      job.setGroupingComparatorClass(groupingComparator.getClass());
    }
  }
  
  public void setCombiner(Reducer<K2, V2, K2, V2> combiner) throws IOException {
    if (combiner instanceof Serializable) {
      SerializableCombinerDelegator.setDelegate(job, combiner);
    } else {
      job.setCombinerClass(combiner.getClass());
    }
  }

  public void setReducer(Reducer<K2, V2, K3, V3> reducer) throws IOException {
    if (reducer instanceof Serializable) {
      SerializableReducerDelegator.setDelegate(job, reducer);
    } else {
      job.setReducerClass(reducer.getClass());
    }
  }
  
  public void setOutputFormat(OutputFormat<K3, V3> outputFormat) throws IOException {
    if (outputFormat instanceof Serializable) {
      SerializableOutputFormatDelegator.setDelegate(job, outputFormat);
    } else {
      job.setOutputFormatClass(outputFormat.getClass());
    }
  }
  
  public void setMapOutputKeyClass(Class<K2> theClass) {
    job.setMapOutputKeyClass(theClass);
  }

  public void setMapOutputValueClass(Class<V2> theClass) {
    job.setMapOutputValueClass(theClass);
  }

  public void setOutputKeyClass(Class<K3> theClass) {
    job.setOutputKeyClass(theClass);
  }

  public void setOutputValueClass(Class<V3> theClass) {
    job.setOutputValueClass(theClass);
  }

  public boolean waitForCompletion(boolean verbose) throws IOException,
      InterruptedException, ClassNotFoundException {
    return job.waitForCompletion(true);
  }
}
