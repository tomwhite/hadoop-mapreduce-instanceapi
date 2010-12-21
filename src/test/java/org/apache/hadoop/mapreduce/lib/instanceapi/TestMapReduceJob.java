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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.instanceapi.MapReduceJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

public class TestMapReduceJob extends TestCase {

  private static String TEST_ROOT_DIR =
    new File(System.getProperty("test.build.data", "/tmp")).toURI()
    .toString().replace(' ', '+');

  private final Path INPUT_DIR = new Path(TEST_ROOT_DIR + "/input");
  private final Path OUTPUT_DIR = new Path(TEST_ROOT_DIR + "/out");
  private final Path INPUT_FILE = new Path(INPUT_DIR , "inp");

  static class GrepMapper extends Mapper<LongWritable, Text, Text, LongWritable>
    implements Serializable {
    
    private Pattern pattern;
    
    public GrepMapper() {
    }
    
    public GrepMapper(String pattern) {
      this.pattern = Pattern.compile(pattern);
    }
    
    protected void map(LongWritable key, Text value,
        Mapper<LongWritable,Text,Text,LongWritable>.Context context)
        throws IOException, InterruptedException {
      String text = value.toString();
      Matcher matcher = pattern.matcher(text);
      while (matcher.find()) {
        context.write(new Text(matcher.group(0)), new LongWritable(1));
      }
    }
  }
  
  private void cleanAndCreateInput(FileSystem fs) throws IOException {
    fs.delete(INPUT_FILE, true);
    fs.delete(OUTPUT_DIR, true);

    OutputStream os = fs.create(INPUT_FILE);

    Writer wr = new OutputStreamWriter(os);
    wr.write("food\n");
    wr.write("drink\n");
    wr.write("foo\n");
    wr.close();
  }
  
  public void testMixedJob() throws Exception {

    Configuration conf = new Configuration();
    Job job = new Job(conf);
    
    FileSystem fs = FileSystem.get(conf);
    cleanAndCreateInput(fs);

    MapReduceJob<LongWritable, Text, Text, LongWritable, Text, LongWritable> mrJob =
      MapReduceJob.newJob(job);
    mrJob.setMapper(new GrepMapper("foo"));
    mrJob.setReducer(new LongSumReducer<Text>());
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    
    FileInputFormat.setInputPaths(job, INPUT_DIR);
    FileOutputFormat.setOutputPath(job, OUTPUT_DIR);

    mrJob.waitForCompletion(true);

    Path[] outputFiles = FileUtil.stat2Paths(
        fs.listStatus(OUTPUT_DIR));
    assertEquals(1, outputFiles.length);
    InputStream is = fs.open(outputFiles[0]);
    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    assertEquals("foo\t2", reader.readLine());
    assertNull(reader.readLine());
    reader.close();
  }
  
  static class StatefulInputFormat extends TextInputFormat
      implements Configurable, Serializable {
    private boolean set;
    public StatefulInputFormat() { }
    public StatefulInputFormat(boolean set) { this.set = set; }
    
    @Override public Configuration getConf() { return null; }
    @Override public void setConf(Configuration c) { assertTrue(set); }
  }

  static class StatefulMapper<K1, V1, K2, V2> extends Mapper<K1, V1, K2, V2>
      implements Configurable, Serializable {
    private boolean set;
    public StatefulMapper() { }
    public StatefulMapper(boolean set) { this.set = set; }
    
    @Override public Configuration getConf() { return null; }
    @Override public void setConf(Configuration c) { assertTrue(set); }
  }

  static class StatefulPartitioner<K2, V2> extends HashPartitioner<K2, V2>
      implements Configurable, Serializable {
    private boolean set;
    public StatefulPartitioner() { }
    public StatefulPartitioner(boolean set) { this.set = set; }
    
    @Override public Configuration getConf() { return null; }
    @Override public void setConf(Configuration c) { assertTrue(set); }
  }

  static class StatefulKeyComparator extends Text.Comparator
      implements Configurable, Serializable {
    private boolean set;
    public StatefulKeyComparator() { }
    public StatefulKeyComparator(boolean set) { this.set = set; }
    
    @Override public Configuration getConf() { return null; }
    @Override public void setConf(Configuration c) { assertTrue(set); }
  }

  static class StatefulValueGroupingComparator extends LongWritable.Comparator
      implements Configurable, Serializable {
    private boolean set;
    public StatefulValueGroupingComparator() { }
    public StatefulValueGroupingComparator(boolean set) { this.set = set; }
    
    @Override public Configuration getConf() { return null; }
    @Override public void setConf(Configuration c) { assertTrue(set); }
  }

  static class StatefulReducer<K2, V2, K3, V3> extends Reducer<K2, V2, K3, V3>
      implements Configurable, Serializable {
    private boolean set;
    public StatefulReducer() { }
    public StatefulReducer(boolean set) { this.set = set; }
    
    @Override public Configuration getConf() { return null; }
    @Override public void setConf(Configuration c) { assertTrue(set); }
  }

  static class StatefulOutputFormat<K, V> extends TextOutputFormat<K, V>
      implements Configurable, Serializable {
    private boolean set;
    public StatefulOutputFormat() { }
    public StatefulOutputFormat(boolean set) { this.set = set; }
    
    @Override public Configuration getConf() { return null; }
    @Override public void setConf(Configuration c) { assertTrue(set); }
  }

  public void testFullyStatefulJob() throws Exception {

    Configuration conf = new Configuration();
    Job job = new Job(conf);
    
    FileSystem fs = FileSystem.get(conf);
    cleanAndCreateInput(fs);

    MapReduceJob<LongWritable, Text, Text, LongWritable, Text, LongWritable> mrJob =
      MapReduceJob.newJob(job);
    mrJob.setInputFormat(new StatefulInputFormat(true));
    mrJob.setMapper(new StatefulMapper(true));
    mrJob.setPartitioner(new StatefulPartitioner(true));
    mrJob.setSortComparator(new StatefulKeyComparator(true));
    mrJob.setGroupingComparator(new StatefulValueGroupingComparator(true));
    mrJob.setCombiner(new StatefulReducer(true));
    mrJob.setReducer(new StatefulReducer(true));
    mrJob.setOutputFormat(new StatefulOutputFormat<Text, LongWritable>(true));
    
    FileInputFormat.setInputPaths(job, INPUT_DIR);
    FileOutputFormat.setOutputPath(job, OUTPUT_DIR);

    mrJob.waitForCompletion(true);

    Path[] outputFiles = FileUtil.stat2Paths(
        fs.listStatus(OUTPUT_DIR));
    assertEquals(1, outputFiles.length);
    InputStream is = fs.open(outputFiles[0]);
    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    assertEquals("0\tfood", reader.readLine());
    assertEquals("5\tdrink", reader.readLine());
    assertEquals("11\tfoo", reader.readLine());
    assertNull(reader.readLine());
    reader.close();
  }
}
