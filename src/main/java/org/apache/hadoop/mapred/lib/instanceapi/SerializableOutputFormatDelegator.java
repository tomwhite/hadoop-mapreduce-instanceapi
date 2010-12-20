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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

@SuppressWarnings("deprecation")
class SerializableOutputFormatDelegator<K3, V3> implements OutputFormat<K3, V3>, JobConfigurable {
  
  static final String BYTES_PROPERTY = "mapred.output.format.class.bytes";

  private OutputFormat<K3, V3> delegate;
  
  public static <K3, V3> void setDelegate(JobConf job,
      OutputFormat<K3, V3> delegate) throws IOException {
    job.setOutputFormat(SerializableOutputFormatDelegator.class);
    SerializableUtil.serializeObject(job, BYTES_PROPERTY, delegate);
  }

  @Override
  public void configure(JobConf job) {
    delegate = SerializableUtil.deserializeObject(job, BYTES_PROPERTY);
  }

  @Override
  public void checkOutputSpecs(FileSystem arg0, JobConf arg1)
      throws IOException {
    delegate.checkOutputSpecs(arg0, arg1);
  }

  @Override
  public RecordWriter<K3, V3> getRecordWriter(FileSystem arg0, JobConf arg1,
      String arg2, Progressable arg3) throws IOException {
    // TODO Auto-generated method stub
    return delegate.getRecordWriter(arg0, arg1, arg2, arg3);
  }

}
