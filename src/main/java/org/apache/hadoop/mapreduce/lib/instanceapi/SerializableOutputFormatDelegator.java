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
import org.apache.hadoop.mapred.lib.instanceapi.SerializableUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

class SerializableOutputFormatDelegator<K3, V3> extends OutputFormat<K3, V3> implements Configurable {

  static final String BYTES_PROPERTY = "mapreduce.job.outputformat.class.bytes";
  
  private Configuration conf;
  private OutputFormat<K3, V3> delegate;
  
  public static <K3, V3> void setDelegate(Job job,
      OutputFormat<K3, V3> delegate) throws IOException {
    job.setOutputFormatClass(SerializableOutputFormatDelegator.class);
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
  public RecordWriter<K3, V3> getRecordWriter(TaskAttemptContext arg0)
      throws IOException, InterruptedException {
    return delegate.getRecordWriter(arg0);
  }
  
  @Override
  public void checkOutputSpecs(JobContext arg0) throws IOException,
      InterruptedException {
    delegate.checkOutputSpecs(arg0);
  }
  
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext arg0)
      throws IOException, InterruptedException {
    return delegate.getOutputCommitter(arg0);
  }
  
}
