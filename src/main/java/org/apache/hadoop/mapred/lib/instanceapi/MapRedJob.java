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
import java.io.Serializable;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.RunningJob;

/**
 * An interface for configuring and running MapReduce jobs. This is an
 * alternative to {@link JobConf}, in that it uses instances rather than classes
 * for configuration.
 * <p>
 * Instances of classes that implement {@link java.io.Serializable} are
 * considered stateful and their configuration state is preserved so that
 * new instantiations of these instances have the same state.
 * <p>
 * For example, consider a {@link Mapper} for implementing grep, which stores
 * the pattern to search for as an instance variable:
 * <pre>
 * public class GrepMapper extends MapReduceBase implements
 *    Mapper&lt;LongWritable, Text, Text, LongWritable&gt;, Serializable {
 *  
 *  private Pattern pattern;
 *  
 *  public GrepMapper() {
 *  }
 *  
 *  public GrepMapper(String pattern) {
 *    this.pattern = Pattern.compile(pattern);
 *  }
 *
 *  public void map(LongWritable key, Text value,
 *      OutputCollector&lt;Text, LongWritable&gt; output, Reporter reporter)
 *      throws IOException {
 *    String text = value.toString();
 *    Matcher matcher = pattern.matcher(text);
 *    while (matcher.find()) {
 *      output.collect(new Text(matcher.group(0)), new LongWritable(1));
 *    }
 *  }
 * }
 * </pre>
 * To use this class an instance of <code>GrepMapper</code> is passed to
 * an instance of <code>MapRedJob</code>.
 * <pre>
 *  MapRedJob&lt;LongWritable, Text, Text, LongWritable, Text, LongWritable&gt; job =
 *    MapRedJob.newJob();
 *  job.setMapper(new GrepMapper("foo"));
 *  job.setReducer(new LongSumReducer&lt;Text&gt;());
 *  job.setOutputKeyClass(Text.class);
 *  job.setOutputValueClass(LongWritable.class);
 *  ...
 *  job.runJob();
 * </pre>
 */
@SuppressWarnings("deprecation")
public class MapRedJob<K1, V1, K2, V2, K3, V3> {

  private JobConf job;

  public static <K1, V1, K2, V2, K3, V3> MapRedJob<K1, V1, K2, V2, K3, V3> newJob()
      throws IOException {
    return new MapRedJob<K1, V1, K2, V2, K3, V3>();
  }

  public static <K1, V1, K2, V2, K3, V3> MapRedJob<K1, V1, K2, V2, K3, V3> newJob(
      JobConf job) throws IOException {
    return new MapRedJob<K1, V1, K2, V2, K3, V3>(job);
  }

  MapRedJob() throws IOException {
    this(new JobConf());
  }

  MapRedJob(JobConf job) throws IOException {
    this.job = job;
  }

  public void setInputFormat(InputFormat<K1, V1> inputFormat)
      throws IOException {
    if (inputFormat instanceof Serializable) {
      SerializableInputFormatDelegator.setDelegate(job, inputFormat);
    } else {
      job.setInputFormat(inputFormat.getClass());
    }
  }

  public void setMapRunnable(MapRunnable<K1, V1, K2, V2> mapRunnable)
      throws IOException {
    if (mapRunnable instanceof Serializable) {
      SerializableMapRunnableDelegator.setDelegate(job, mapRunnable);
    } else {
      job.setMapRunnerClass(mapRunnable.getClass());
    }
  }

  public void setMapper(Mapper<K1, V1, K2, V2> mapper) throws IOException {
    if (mapper instanceof Serializable) {
      SerializableMapperDelegator.setDelegate(job, mapper);
    } else {
      job.setMapperClass(mapper.getClass());
    }
  }

  public void setPartitioner(Partitioner<K2, V2> partitioner)
      throws IOException {
    if (partitioner instanceof Serializable) {
      SerializablePartitionerDelegator.setDelegate(job, partitioner);
    } else {
      job.setPartitionerClass(partitioner.getClass());
    }
  }

  public void setOutputKeyComparator(RawComparator<K2> keyComparator)
      throws IOException {
    if (keyComparator instanceof Serializable) {
      SerializableOutputKeyComparatorDelegator.setDelegate(job, keyComparator);
    } else {
      job.setOutputKeyComparatorClass(keyComparator.getClass());
    }
  }

  public void setOutputValueGroupingComparator(
      RawComparator<V2> valueGroupingComparator) throws IOException {
    if (valueGroupingComparator instanceof Serializable) {
      SerializableOutputValueGroupingComparatorDelegator.setDelegate(job,
          valueGroupingComparator);
    } else {
      job.setOutputValueGroupingComparator(valueGroupingComparator.getClass());
    }
  }

  public void setCombiner(Reducer<K2, V2, K2, V2> combiner) throws IOException {
    if (combiner instanceof Serializable) {
      SerializableCombinerDelegator.setDelegate(job, combiner);
    } else {
      job.setCombinerClass(combiner.getClass());
    }
  }

  public void setReducer(Reducer<K2, V2, K3, V3> reducer) throws IOException {
    if (reducer instanceof Serializable) {
      SerializableReducerDelegator.setDelegate(job, reducer);
    } else {
      job.setReducerClass(reducer.getClass());
    }
  }

  public void setOutputFormat(OutputFormat<K3, V3> outputFormat)
      throws IOException {
    if (outputFormat instanceof Serializable) {
      SerializableOutputFormatDelegator.setDelegate(job, outputFormat);
    } else {
      job.setOutputFormat(outputFormat.getClass());
    }
  }

  public void setOutputCommmitter(OutputCommitter outputCommitter)
      throws IOException {
    if (outputCommitter instanceof Serializable) {
      SerializableOutputCommitterDelegator.setDelegate(job, outputCommitter);
    } else {
      job.setOutputCommitter(outputCommitter.getClass());
    }
  }

  public void setMapOutputKeyClass(Class<K2> theClass) {
    job.setMapOutputKeyClass(theClass);
  }

  public void setMapOutputValueClass(Class<V2> theClass) {
    job.setMapOutputValueClass(theClass);
  }

  public void setOutputKeyClass(Class<K3> theClass) {
    job.setOutputKeyClass(theClass);
  }

  public void setOutputValueClass(Class<V3> theClass) {
    job.setOutputValueClass(theClass);
  }

  public RunningJob runJob() throws IOException {
    return JobClient.runJob(job);
  }

}
