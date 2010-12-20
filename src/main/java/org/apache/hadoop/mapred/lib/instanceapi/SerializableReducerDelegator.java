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
import java.util.Iterator;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
class SerializableReducerDelegator<K2, V2, K3, V3> implements Reducer<K2, V2, K3, V3>, Configurable {
  
  static final String REDUCE_CLASS_BYTES = "mapred.reducer.class.bytes";

  private Configuration conf;
  private Reducer<K2, V2, K3, V3> delegate;
  
  public static <K2, V2, K3, V3> void setDelegate(JobConf job,
      Reducer<K2, V2, K3, V3> delegate) throws IOException {
    job.setReducerClass(SerializableReducerDelegator.class);
    SerializableUtil.serializeObject(job, REDUCE_CLASS_BYTES, delegate);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    delegate = SerializableUtil.deserializeObject(conf, REDUCE_CLASS_BYTES);
    this.conf = conf;
  }

  @Override
  public void configure(JobConf conf) {
    delegate.configure(conf);
  }

  @Override
  public void reduce(K2 arg0, Iterator<V2> arg1, OutputCollector<K3, V3> arg2,
      Reporter arg3) throws IOException {
    delegate.reduce(arg0, arg1, arg2, arg3);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

}
