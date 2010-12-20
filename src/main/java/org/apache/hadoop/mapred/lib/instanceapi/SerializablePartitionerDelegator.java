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

package org.apache.hadoop.mapred.lib.instanceapi;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.Partitioner;

@SuppressWarnings("deprecation")
class SerializablePartitionerDelegator<K2, V2> implements Partitioner<K2, V2>, JobConfigurable {
  
  static final String PARTITIONER_CLASS_BYTES = "mapred.partitioner.class.bytes";

  private Partitioner<K2, V2> delegate;
  
  public static <K2, V2> void setDelegate(JobConf job,
      Partitioner<K2, V2> delegate) throws IOException {
    job.setPartitionerClass(SerializablePartitionerDelegator.class);
    SerializableUtil.serializeObject(job, PARTITIONER_CLASS_BYTES, delegate);
  }

  @Override
  public void configure(JobConf job) {
    delegate = SerializableUtil.deserializeObject(job, PARTITIONER_CLASS_BYTES);
  }

  @Override
  public int getPartition(K2 arg0, V2 arg1, int arg2) {
    return delegate.getPartition(arg0, arg1, arg2);
  }

}
