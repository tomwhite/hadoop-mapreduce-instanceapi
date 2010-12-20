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

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.SerializableMapperDelegator;

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
  
  public void setMapper(Mapper<K1, V1, K2, V2> mapper) throws IOException {
    if (mapper instanceof Serializable) {
      SerializableMapperDelegator.setDelegate(job, mapper);
    } else {
      job.setMapperClass(mapper.getClass());
    }
  }

  public void setReducer(Reducer<K2, V2, K3, V3> reducer) throws IOException {
    if (reducer instanceof Serializable) {
      // TODO
    } else {
      job.setReducerClass(reducer.getClass());
    }
  }
  public void runJob() throws IOException, InterruptedException,
      ClassNotFoundException {
    job.waitForCompletion(true);
  }
}
