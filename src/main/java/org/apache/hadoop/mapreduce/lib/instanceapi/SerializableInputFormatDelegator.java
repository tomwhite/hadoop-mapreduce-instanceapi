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
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.lib.instanceapi.SerializableUtil;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

class SerializableInputFormatDelegator<K1, V1> extends InputFormat<K1, V1> implements Configurable {

  static final String BYTES_PROPERTY = "mapreduce.job.inputformat.class.bytes";
  
  private Configuration conf;
  private InputFormat<K1, V1> delegate;
  
  public static <K1, V1> void setDelegate(Job job,
      InputFormat<K1, V1> delegate) throws IOException {
    job.setInputFormatClass(SerializableInputFormatDelegator.class);
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
  
  @Override
  public List<InputSplit> getSplits(JobContext arg0) throws IOException,
      InterruptedException {
    return delegate.getSplits(arg0);
  }
  
  @Override
  public RecordReader<K1, V1> createRecordReader(InputSplit arg0,
      TaskAttemptContext arg1) throws IOException, InterruptedException {
    return delegate.createRecordReader(arg0, arg1);
  }
}
