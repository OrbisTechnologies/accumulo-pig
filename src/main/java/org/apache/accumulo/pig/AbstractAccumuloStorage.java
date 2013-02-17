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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;

import cloudbase.core.client.mapreduce.CloudbaseInputFormat;
import cloudbase.core.client.mapreduce.CloudbaseOutputFormat;
import cloudbase.core.data.Key;
import cloudbase.core.data.Mutation;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.security.Authorizations;
import cloudbase.core.util.Pair;

/**
 * A LoadStoreFunc for retrieving data from and storing data to Accumulo
 *
 * A Key/Val pair will be returned as tuples: (key, colfam, colqual, colvis,
 * timestamp, value). All fields except timestamp are DataByteArray, timestamp
 * is a long.
 *
 * Tuples can be written in 2 forms: (key, colfam, colqual, colvis, value) OR
 * (key, colfam, colqual, value)
 *
 */
public abstract class AbstractAccumuloStorage extends LoadFunc implements
        StoreFuncInterface {

  private static final Log LOG = LogFactory
          .getLog(AbstractAccumuloStorage.class);
  private Configuration conf;
  private RecordReader<Key, Value> reader;
  private RecordWriter<Text, Mutation> writer;
  String inst;
  String zookeepers;
  String user;
  String password;
  String table;
  Text tableName;
  String auths;
  Authorizations authorizations;
  List<Pair<Text, Text>> cfcqPairs = new LinkedList<Pair<Text, Text>>();
  String start = null;
  String end = null;
  int maxWriteThreads = 10;
  long maxBufferSize = 10 * 1000 * 1000;
  int maxLatency = 10 * 1000;

  public AbstractAccumuloStorage() {
  }

  @Override
  public Tuple getNext() throws IOException {
    try {
      // load the next pair
      if (!reader.nextKeyValue()) {
        return null;
      }

      Key key = (Key) reader.getCurrentKey();
      Value value = (Value) reader.getCurrentValue();
      assert key != null && value != null;
      return getTuple(key, value);
    } catch (InterruptedException e) {
      throw new IOException(e.getMessage());
    }
  }

  protected abstract Tuple getTuple(Key key, Value value) throws IOException;

  @Override
  public InputFormat getInputFormat() {
    return new CloudbaseInputFormat();
  }

  @Override
  public void prepareToRead(RecordReader reader, PigSplit split) {
    this.reader = reader;
  }

  private void setLocationFromUri(String location) throws IOException {
    // ex: accumulo://table1?instance=myinstance&user=root&password=secret
    // &zookeepers=127.0.0.1:2181&auths=PRIVATE,PUBLIC
    // &columns=col1|cq1,col2|cq2&start=abc&end=z
    String columns = "";
    try {
      if (!location.startsWith("accumulo://")) {
        throw new Exception("Bad scheme.");
      }
      String[] urlParts = location.split("\\?");
      if (urlParts.length > 1) {
        for (String param : urlParts[1].split("&")) {
          String[] pair = param.split("=");
          if (pair[0].equals("instance")) {
            inst = pair[1];
          } else if (pair[0].equals("user")) {
            user = pair[1];
          } else if (pair[0].equals("password")) {
            password = pair[1];
          } else if (pair[0].equals("zookeepers")) {
            zookeepers = pair[1];
          } else if (pair[0].equals("auths")) {
            auths = pair[1];
          } else if (pair[0].equals("columns")) {
            columns = pair[1];
          } else if (pair[0].equals("start")) {
            start = pair[1];
          } else if (pair[0].equals("end")) {
            end = pair[1];
          } else if (pair[0].equals("write_buffer_size_bytes")) {
            maxBufferSize = Long.parseLong(pair[1]);
          } else if (pair[0].equals("write_threads")) {
            maxWriteThreads = Integer.parseInt(pair[1]);
          } else if (pair[0].equals("write_latency_ms")) {
            maxLatency = Integer.parseInt(pair[1]);
          }
        }
      }
      String[] parts = urlParts[0].split("/+");
      table = parts[1];
      tableName = new Text(table);

      if (auths == null || auths.equals("")) {
        authorizations = new Authorizations();
      } else {
        authorizations = new Authorizations(auths.split(","));
      }

      if (!columns.equals("")) {
        for (String cfCq : columns.split(",")) {
          if (cfCq.contains("|")) {
            String[] c = cfCq.split("\\|");
            cfcqPairs
                    .add(new Pair<Text, Text>(new Text(c[0]), new Text(c[1])));
          } else {
            cfcqPairs.add(new Pair<Text, Text>(new Text(
                    cfCq), null));
          }
        }
      }

    } catch (Exception e) {
      throw new IOException("Expected 'accumulo://<table>["
              + "?instance=<instanceName>"
              + "&user=<user>&password=<password>"
              + "&zookeepers=<zookeepers>"
              + "&auths=<authorizations>"
              + "&[start=startRow,end=endRow,columns=[cf1|cq1,cf2|cq2,...],"
              + "write_buffer_size_bytes=10000000,"
              + "write_threads=10,write_latency_ms=30000]]': "
              + e.getMessage());
    }
  }

  protected RecordWriter<Text, Mutation> getWriter() {
    return writer;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    conf = job.getConfiguration();
    setLocationFromUri(location);

    if (!conf.getBoolean(CloudbaseInputFormat.class.getSimpleName()
            + ".configured", false)) {
      CloudbaseInputFormat.setInputInfo(job, user, password.getBytes(), table,
                                        authorizations);
      CloudbaseInputFormat.setZooKeeperInstance(job, inst, zookeepers);
      if (cfcqPairs.size() > 0) {
        LOG.info("columns: " + cfcqPairs);
        CloudbaseInputFormat.fetchColumns(job, cfcqPairs);
      }

      CloudbaseInputFormat.setRanges(
              job, Collections.singleton(new Range(start, end)));
      configureInputFormat(conf);
    }
  }

  protected void configureInputFormat(Configuration conf) {
  }

  protected void configureOutputFormat(Configuration conf) {
  }

  @Override
  public String relativeToAbsolutePath(String location, Path curDir) throws
          IOException {
    return location;
  }

  @Override
  public void setUDFContextSignature(String signature) {
  }

  /* StoreFunc methods */
  @Override
  public void setStoreFuncUDFContextSignature(String signature) {
  }

  @Override
  public String relToAbsPathForStoreLocation(String location, Path curDir)
          throws IOException {
    return relativeToAbsolutePath(location, curDir);
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    conf = job.getConfiguration();
    setLocationFromUri(location);

    if (!conf.getBoolean(CloudbaseOutputFormat.class.getSimpleName()
            + ".configured", false)) {
      CloudbaseOutputFormat.setOutputInfo(job, user, password.getBytes(), true,
                                          table);
      CloudbaseOutputFormat.setZooKeeperInstance(job, inst, zookeepers);
      CloudbaseOutputFormat.setMaxLatency(job, maxLatency);
      CloudbaseOutputFormat.setMaxMutationBufferSize(job, maxBufferSize);
      CloudbaseOutputFormat.setMaxWriteThreads(job, maxWriteThreads);
      configureOutputFormat(conf);
    }
  }

  @Override
  public OutputFormat getOutputFormat() {
    return new CloudbaseOutputFormat();
  }

  @Override
  public void checkSchema(ResourceSchema schema) throws IOException {
    // we don't care about types, they all get casted to ByteBuffers
  }

  @Override
  public void prepareToWrite(RecordWriter writer) {
    this.writer = writer;
  }

  public abstract Collection<Mutation> getMutations(Tuple tuple) throws
          ExecException, IOException;

  @Override
  public void putNext(Tuple tuple) throws ExecException, IOException {
    Collection<Mutation> muts = getMutations(tuple);
    for (Mutation mut : muts) {
      try {
        getWriter().write(tableName, mut);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
  }

  @Override
  public void cleanupOnFailure(String failure, Job job) {
  }
}
