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

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;

@SuppressWarnings("deprecation")
class SerializableOutputCommitterDelegator extends OutputCommitter implements JobConfigurable {
  
  static final String BYTES_PROPERTY = "mapred.output.committer.class.bytes";

  private OutputCommitter delegate;
  
  public static void setDelegate(JobConf job,
      OutputCommitter delegate) throws IOException {
    job.setOutputCommitter(SerializableOutputCommitterDelegator.class);
    SerializableUtil.serializeObject(job, BYTES_PROPERTY, delegate);
  }

  @Override
  public void configure(JobConf job) {
    delegate = SerializableUtil.deserializeObject(job, BYTES_PROPERTY);
  }

  @Override
  public void abortTask(TaskAttemptContext arg0) throws IOException {
    delegate.abortTask(arg0);
  }

  @Override
  public void cleanupJob(JobContext arg0) throws IOException {
    delegate.cleanupJob(arg0);
  }

  @Override
  public void commitTask(TaskAttemptContext arg0) throws IOException {
    delegate.commitTask(arg0);
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext arg0) throws IOException {
    return delegate.needsTaskCommit(arg0);
  }

  @Override
  public void setupJob(JobContext arg0) throws IOException {
    delegate.setupJob(arg0);
  }

  @Override
  public void setupTask(TaskAttemptContext arg0) throws IOException {
    delegate.setupTask(arg0);
  }

}
