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

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;

@SuppressWarnings("deprecation")
class SerializableOutputKeyComparatorDelegator<K2> implements RawComparator<K2>, JobConfigurable {
  
  static final String BYTES_PROPERTY = "mapreduce.job.output.key.comparator.class.bytes";

  private RawComparator<K2> delegate;
  
  public static <K2> void setDelegate(JobConf job,
      RawComparator<K2> delegate) throws IOException {
    job.setOutputKeyComparatorClass(SerializableOutputKeyComparatorDelegator.class);
    SerializableUtil.serializeObject(job, BYTES_PROPERTY, delegate);
  }
  
  @Override
  public void configure(JobConf job) {
    delegate = SerializableUtil.deserializeObject(job, BYTES_PROPERTY);
  }

  @Override
  public int compare(K2 o1, K2 o2) {
    return delegate.compare(o1, o2);
  }

  @Override
  public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4,
      int arg5) {
    return delegate.compare(arg0, arg1, arg2, arg3, arg4, arg5);
  }

}
