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

public class SerializableMapperDelegator<K1, V1, K2, V2> extends Mapper<K1, V1, K2, V2> implements Configurable {

  public static final String MAP_CLASS_BYTES = "mapreduce.mapper.class.bytes";
  
  private Configuration conf;
  private Mapper<K1, V1, K2, V2> delegate;
  
  public static <K1, V1, K2, V2> void setDelegate(Job job,
      Mapper<K1, V1, K2, V2> delegate) throws IOException {
    job.setMapperClass(SerializableMapperDelegator.class);
    SerializableUtil.serializeObject(job.getConfiguration(), MAP_CLASS_BYTES,
        delegate);
  }
  
  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    delegate = SerializableUtil.deserializeObject(conf, MAP_CLASS_BYTES);
    this.conf = conf;
  }
  
  protected void setup(Mapper<K1,V1,K2,V2>.Context context) throws IOException, InterruptedException {
    delegate.setup(context);
  }
  
  protected void map(K1 key, V1 value, Mapper<K1,V1,K2,V2>.Context context) throws IOException, InterruptedException {
    delegate.map(key, value, context);
  }
  
  protected void cleanup(Mapper<K1,V1,K2,V2>.Context context) throws IOException, InterruptedException {
    delegate.cleanup(context);
  }
  
  public void run(Mapper<K1,V1,K2,V2>.Context context) throws IOException, InterruptedException {
    delegate.run(context);
  }

}
