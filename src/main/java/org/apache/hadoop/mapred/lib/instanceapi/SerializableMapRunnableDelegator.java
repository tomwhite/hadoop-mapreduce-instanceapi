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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
class SerializableMapRunnableDelegator<K1, V1, K2, V2> implements MapRunnable<K1, V1, K2, V2>, Configurable {
  
  static final String BYTES_PROPERTY = "mapred.map.runner.class.bytes";

  private Configuration conf;
  private MapRunnable<K1, V1, K2, V2> delegate;
  
  public static <K1, V1, K2, V2> void setDelegate(JobConf job, MapRunnable<K1, V1, K2, V2> delegate)
      throws IOException {
    job.setMapRunnerClass(SerializableMapRunnableDelegator.class);
    SerializableUtil.serializeObject(job, BYTES_PROPERTY, delegate);
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
  public void configure(JobConf arg0) {
    delegate.configure(arg0);
  }

  @Override
  public void run(RecordReader<K1, V1> arg0, OutputCollector<K2, V2> arg1,
      Reporter arg2) throws IOException {
    delegate.run(arg0, arg1, arg2);
  }


}
