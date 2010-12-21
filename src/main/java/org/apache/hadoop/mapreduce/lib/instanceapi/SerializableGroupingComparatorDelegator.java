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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.lib.instanceapi.SerializableUtil;
import org.apache.hadoop.mapreduce.Job;

class SerializableGroupingComparatorDelegator<V2> implements RawComparator<V2>, Configurable {
  
  static final String BYTES_PROPERTY = "mapreduce.job.output.group.comparator.class.bytes";

  private Configuration conf;
  private RawComparator<V2> delegate;
  
  public static <V2> void setDelegate(Job job,
      RawComparator<V2> delegate) throws IOException {
    job.setGroupingComparatorClass(SerializableGroupingComparatorDelegator.class);
    SerializableUtil.serializeObject(job.getConfiguration(), BYTES_PROPERTY, delegate);
  }
  
  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    delegate = SerializableUtil.deserializeObject(conf, BYTES_PROPERTY);
    this.conf = conf;
  }

  @Override
  public int compare(V2 o1, V2 o2) {
    return delegate.compare(o1, o2);
  }

  @Override
  public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4,
      int arg5) {
    return delegate.compare(arg0, arg1, arg2, arg3, arg4, arg5);
  }

}
