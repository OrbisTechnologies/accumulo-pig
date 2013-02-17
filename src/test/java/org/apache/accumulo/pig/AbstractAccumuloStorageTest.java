/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.accumulo.pig;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import cloudbase.core.client.mapreduce.CloudbaseInputFormat;
import cloudbase.core.client.mapreduce.CloudbaseOutputFormat;
import cloudbase.core.data.Key;
import cloudbase.core.data.Mutation;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.security.Authorizations;
import cloudbase.core.util.Pair;

public class AbstractAccumuloStorageTest {

  public Job getExpectedLoadJob(
          String inst, String zookeepers, String user,
          String password, String table,
          String start, String end,
          Authorizations authorizations,
          List<Pair<Text, Text>> columnFamilyColumnQualifierPairs)
          throws IOException {
    Collection<Range> ranges = new LinkedList<Range>();
    ranges.add(new Range(start, end));

    Job expected = new Job();
    CloudbaseInputFormat.setInputInfo(expected, user, password.getBytes(),
                                      table, authorizations);
    CloudbaseInputFormat.setZooKeeperInstance(expected, inst, zookeepers);
    CloudbaseInputFormat.fetchColumns(expected,
                                      columnFamilyColumnQualifierPairs);
    CloudbaseInputFormat.setRanges(expected, ranges);
    return expected;
  }

  public Job getDefaultExpectedLoadJob() throws IOException {
    String inst = "myinstance";
    String zookeepers = "127.0.0.1:2181";
    String user = "root";
    String password = "secret";
    String table = "table1";
    String start = "abc";
    String end = "z";
    Authorizations authorizations = new Authorizations("PRIVATE,PUBLIC".split(
            ","));

    List<Pair<Text, Text>> columnFamilyColumnQualifierPairs =
            new LinkedList<Pair<Text, Text>>();
    columnFamilyColumnQualifierPairs.add(new Pair<Text, Text>(new Text("col1"),
                                                              new Text("cq1")));
    columnFamilyColumnQualifierPairs.add(new Pair<Text, Text>(new Text("col2"),
                                                              new Text("cq2")));
    columnFamilyColumnQualifierPairs.add(new Pair<Text, Text>(new Text("col3"),
                                                              null));

    Job expected = getExpectedLoadJob(inst, zookeepers, user, password, table,
                                      start, end, authorizations,
                                      columnFamilyColumnQualifierPairs);
    return expected;
  }

  public Job getExpectedStoreJob(String inst, String zookeepers, String user,
                                 String password, String table,
                                 long maxWriteBufferSize, int writeThreads,
                                 int maxWriteLatencyMS) throws IOException {
    Job expected = new Job();
    Configuration expectedConf = expected.getConfiguration();
    CloudbaseOutputFormat.setOutputInfo(expected, user, password.getBytes(),
                                        true, table);
    CloudbaseOutputFormat.setZooKeeperInstance(expected, inst, zookeepers);
    CloudbaseOutputFormat.setMaxLatency(expected, maxWriteLatencyMS);
    CloudbaseOutputFormat.setMaxMutationBufferSize(expected,
                                                   maxWriteBufferSize);
    CloudbaseOutputFormat.setMaxWriteThreads(expected, writeThreads);

    return expected;
  }

  public Job getDefaultExpectedStoreJob() throws IOException {
    String inst = "myinstance";
    String zookeepers = "127.0.0.1:2181";
    String user = "root";
    String password = "secret";
    String table = "table1";
    long maxWriteBufferSize = 1234000;
    int writeThreads = 7;
    int maxWriteLatencyMS = 30000;

    Job expected = getExpectedStoreJob(inst, zookeepers, user, password, table,
                                       maxWriteBufferSize, writeThreads,
                                       maxWriteLatencyMS);
    return expected;
  }

  public String getDefaultLoadLocation() {
    return "accumulo://table1"
            + "?instance=myinstance"
            + "&user=root"
            + "&password=secret"
            + "&zookeepers=127.0.0.1:2181"
            + "&auths=PRIVATE,PUBLIC"
            + "&columns=col1|cq1,col2|cq2,col3"
            + "&start=abc"
            + "&end=z";
  }

  public String getDefaultStoreLocation() {
    return "accumulo://table1"
            + "?instance=myinstance"
            + "&user=root"
            + "&password=secret"
            + "&zookeepers=127.0.0.1:2181"
            + "&write_buffer_size_bytes=1234000"
            + "&write_threads=7"
            + "&write_latency_ms=30000";
  }

  public AbstractAccumuloStorage getAbstractAccumuloStorage() {
    AbstractAccumuloStorage s = new AbstractAccumuloStorage() {
      @Override
      public Collection<Mutation> getMutations(Tuple tuple) {
        return null;
      }

      @Override
      protected Tuple getTuple(Key key, Value value) throws IOException {
        return null;
      }
    };
    return s;
  }

  @Test
  public void testSetLoadLocation() throws IOException {
    AbstractAccumuloStorage s = getAbstractAccumuloStorage();

    Job actual = new Job();
    s.setLocation(getDefaultLoadLocation(), actual);
    Configuration actualConf = actual.getConfiguration();

    Job expected = getDefaultExpectedLoadJob();
    Configuration expectedConf = expected.getConfiguration();

    TestUtils.assertConfigurationsEqual(expectedConf, actualConf);
  }

  @Test
  public void testSetStoreLocation() throws IOException {
    AbstractAccumuloStorage s = getAbstractAccumuloStorage();

    Job actual = new Job();
    s.setStoreLocation(getDefaultStoreLocation(), actual);
    Configuration actualConf = actual.getConfiguration();

    Job expected = getDefaultExpectedStoreJob();
    Configuration expectedConf = expected.getConfiguration();

    TestUtils.assertConfigurationsEqual(expectedConf, actualConf);
  }
}
