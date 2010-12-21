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

package org.apache.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.lib.instanceapi.SerializableUtil;

public class SerializableCombinerDelegator<K2, V2> extends Reducer<K2, V2, K2, V2> implements Configurable {

  static final String BYTES_PROPERTY = "mapreduce.combiner.class.bytes";
  
  private Configuration conf;
  private Reducer<K2, V2, K2, V2> delegate;
  
  public static <K2, V2> void setDelegate(Job job,
      Reducer<K2, V2, K2, V2> delegate) throws IOException {
    job.setCombinerClass(SerializableCombinerDelegator.class);
    SerializableUtil.serializeObject(job.getConfiguration(), BYTES_PROPERTY,
        delegate);
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
  
  protected void setup(org.apache.hadoop.mapreduce.Reducer<K2,V2,K2,V2>.Context context) throws IOException ,InterruptedException {
    delegate.setup(context);
  }

  protected void reduce(K2 arg0, java.lang.Iterable<V2> arg1, org.apache.hadoop.mapreduce.Reducer<K2,V2,K2,V2>.Context arg2) throws IOException ,InterruptedException {
    delegate.reduce(arg0, arg1, arg2);
  }
  
  protected void cleanup(org.apache.hadoop.mapreduce.Reducer<K2,V2,K2,V2>.Context context) throws IOException ,InterruptedException {
    delegate.cleanup(context);
  }
  
  public void run(org.apache.hadoop.mapreduce.Reducer<K2,V2,K2,V2>.Context context) throws IOException ,InterruptedException {
    delegate.run(context);
  }
}
