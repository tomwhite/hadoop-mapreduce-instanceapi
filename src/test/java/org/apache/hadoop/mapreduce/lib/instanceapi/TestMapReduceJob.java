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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.instanceapi.MapReduceJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
  
  public void testMapReduceJob() throws Exception {

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

    mrJob.runJob();

    Path[] outputFiles = FileUtil.stat2Paths(
        fs.listStatus(OUTPUT_DIR));
    assertEquals(1, outputFiles.length);
    InputStream is = fs.open(outputFiles[0]);
    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    assertEquals("foo\t2", reader.readLine());
    assertNull(reader.readLine());
    reader.close();
  }
  
}
