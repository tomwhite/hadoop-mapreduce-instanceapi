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

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
class SerializableInputFormatDelegator<K1, V1> implements InputFormat<K1, V1>, JobConfigurable {
  
  static final String BYTES_PROPERTY = "mapred.input.format.class.bytes";

  private InputFormat<K1, V1> delegate;
  
  public static <K1, V1> void setDelegate(JobConf job,
      InputFormat<K1, V1> delegate) throws IOException {
    job.setInputFormat(SerializableInputFormatDelegator.class);
    SerializableUtil.serializeObject(job, BYTES_PROPERTY, delegate);
  }

  @Override
  public void configure(JobConf job) {
    delegate = SerializableUtil.deserializeObject(job, BYTES_PROPERTY);
  }

  @Override
  public RecordReader<K1, V1> getRecordReader(InputSplit arg0, JobConf arg1,
      Reporter arg2) throws IOException {
    return delegate.getRecordReader(arg0, arg1, arg2);
  }

  @Override
  public InputSplit[] getSplits(JobConf arg0, int arg1) throws IOException {
    return delegate.getSplits(arg0, arg1);
  }


}
