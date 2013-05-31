/*
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.RowSortedTable;
import com.google.common.collect.TreeBasedTable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloRowInputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase.RangeInputSplit;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.TimestampFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadPushDown;
import org.apache.pig.LoadStoreCaster;
import org.apache.pig.OrderedLoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

public class AccumuloStorage extends LoadFunc
        implements LoadPushDown, OrderedLoadFunc, StoreFuncInterface {

    private final static Log LOG = LogFactory.getLog(AccumuloStorage.class);
    private final static Options validOptions = new Options();
    private final static CommandLineParser parser = new GnuParser();
    private final static String STRING_CASTER = "UTF8StorageConverter";
    private final static String BYTE_CASTER = "AccumuloBinaryConverter";
    private final static String CASTER_PROPERTY = "pig.accumulo.caster";
    private final static String ASTERISK = "*";
    private final static String COLON = ":";
    private final static String ACCUMULO_CONFIG_SET = "accumulo.config.set";

    static {
        validOptions.addOption("loadKey", false, "Load Key");
        validOptions.addOption("begin", true,
                               "Records must be greater than this value ");
        validOptions.addOption("end", true,
                               "Records must be less than this value");
        validOptions.addOption("isolation", false,
                               "Enable isolation on the table");
        validOptions.addOption("maxVersions", true,
                               "Max Versions to export");
        validOptions.addOption(
                "minTimestamp", true,
                "Record must have timestamp greater or equal to this value");
        validOptions.addOption("maxTimestamp", true,
                               "Record must have timestamp less then this value");
        validOptions.addOption("timestamp", true,
                               "Record must have timestamp equal to this value");
        validOptions.addOption(
                "caster", true,
                "Caster to use for converting values. A class name, "
                + "AccumuloBinaryConverter, or Utf8StorageConverter. "
                + "For storage, casters must implement LoadStoreCaster.");

    }
    private List<ColumnInfo> columnInfo;
    private boolean loadRowKey;
    private String contextSignature;
    private ResourceSchema schema;
    private RecordReader<Text, PeekingIterator<Entry<Key, Value>>> reader;
    private RecordWriter<Text, Mutation> writer;
    private AccumuloOutputFormat outputFormat;
    private Object requiredFieldList;
    // Immutable fields
    private final CommandLine configuredOptions;
    private final LoadCaster caster;
    private final boolean isolation;
    private final String delimiter;
    private final long maxTimestamp;
    private final long minTimestamp;
    private final int maxVersions;
    private boolean jobConfInitialized;

    public AccumuloStorage() throws ParseException, IOException {
        this("", "");
    }

    public AccumuloStorage(String columnList) throws ParseException, IOException {
        this(columnList, "");
    }

    public AccumuloStorage(String columnList, String optString)
            throws ParseException, IOException {

        String[] optsArr = optString.split(" ");
        try {
            configuredOptions = parser.parse(validOptions, optsArr);
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(
                    "[-loadKey] [-begin] [-end] [-isolation] [-delim] "
                    + "[-minTimestamp] [-maxTimestamp] [-timestamp]",
                    validOptions);
            throw e;
        }

        this.loadRowKey = configuredOptions.hasOption("loadKey");

        this.delimiter = (configuredOptions.hasOption("delim"))
                ? configuredOptions.getOptionValue("delim")
                : " ";

        this.isolation = configuredOptions.hasOption("isolation");

        this.maxVersions = (configuredOptions.hasOption("maxVersions"))
                ? Integer.parseInt(configuredOptions.getOptionValue("maxVersions"))
                : 1;

        setColumnInfoList(parseColumnList(columnList, delimiter));

        String defaultCaster = UDFContext.getUDFContext().getClientSystemProps()
                .getProperty(CASTER_PROPERTY, STRING_CASTER);
        String casterOption = configuredOptions.getOptionValue(
                "caster", defaultCaster);
        if (STRING_CASTER.equalsIgnoreCase(casterOption)) {
            caster = new Utf8StorageConverter();
        } else if (BYTE_CASTER.equalsIgnoreCase(casterOption)) {
            caster = new AccumuloBinaryConverter();
        } else {
            try {
                caster = (LoadCaster) PigContext.instantiateFuncFromSpec(casterOption);
            } catch (ClassCastException e) {
                LOG.error("Configured caster does not implement LoadCaster interface.");
                throw new IOException(e);
            } catch (RuntimeException e) {
                LOG.error("Configured caster class not found.", e);
                throw new IOException(e);
            }
        }
        LOG.debug("Using caster " + caster.getClass());

        if (configuredOptions.hasOption("minTimestamp")) {
            minTimestamp = Long.parseLong(configuredOptions.getOptionValue(
                    "minTimestamp"));
        } else {
            minTimestamp = Long.MIN_VALUE;
        }

        if (configuredOptions.hasOption("maxTimestamp")) {
            maxTimestamp = Long.parseLong(configuredOptions.getOptionValue(
                    "maxTimestamp"));
        } else {
            maxTimestamp = Long.MAX_VALUE;
        }

    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        job.getConfiguration().setBoolean("pig.noSplitCombination", true);

        initializeJobConfig(job);

        String tablename = location;
        if (location.startsWith("accumulo://")) {
            tablename = location.substring(11);
        }

        // Get accumulo connection information from job config
        String user = job.getConfiguration().get("accumulo.user");
        byte[] passwd = job.getConfiguration().get("accumulo.passwd").getBytes();
        String authsStr = job.getConfiguration().get("accumulo.auths");
        Authorizations auths = new Authorizations(authsStr.split(","));
        String instance = job.getConfiguration().get("accumulo.instance");
        String zookeepers = job.getConfiguration().get("accumulo.zookeepers");

        try {
            AccumuloRowInputFormat.setZooKeeperInstance(job.getConfiguration(), instance, zookeepers);
            AccumuloRowInputFormat.setInputInfo(job.getConfiguration(), user, passwd, tablename, auths);
//            AccumuloRowInputFormat.setIsolated(job.getConfiguration(), this.isolation);
//            AccumuloRowInputFormat.setMaxVersions(job.getConfiguration(), this.maxVersions);
        } catch (IllegalStateException ex) {
            // Accumulo is dumb!!
            // Ignore
        }

        if (configuredOptions.hasOption("begin")
                || configuredOptions.hasOption("end")) {
            String begin = configuredOptions.getOptionValue("begin");
            String end = configuredOptions.getOptionValue("end");
            Range range = new Range(begin, end);
            AccumuloRowInputFormat.setRanges(job.getConfiguration(), Collections.singleton(range));
        }

        String projectedFields = getUDFProperties().getProperty(
                projectedFieldsName());
        if (projectedFields != null) {
            // update columnInfo_
            pushProjection((RequiredFieldList) ObjectSerializer.deserialize(
                    projectedFields));
        }

        Collection<Pair<Text, Text>> columnsToFetch = Lists.newArrayList();
        if (!columnInfo.isEmpty()) {
            for (ColumnInfo colInfo : columnInfo) {
                if (colInfo.isColumnMap()) {
                    // Column Family only
                    Text cf = new Text(colInfo.getColumnFamily());
                    columnsToFetch.add(new Pair<Text, Text>(cf, null));
                } else {
                    Text cf = new Text(colInfo.getColumnFamily());
                    Text cq = new Text(colInfo.getColumnName());
                    columnsToFetch.add(new Pair<Text, Text>(cf, cq));
                }
            }
        }
        AccumuloRowInputFormat.fetchColumns(job.getConfiguration(), columnsToFetch);

        if (configuredOptions.hasOption("maxTimestamp") || configuredOptions.hasOption("minTimestamp")) {
            IteratorSetting timeFilter = new IteratorSetting(1, TimestampFilter.class);
            TimestampFilter.setRange(timeFilter, minTimestamp, maxTimestamp);
            AccumuloRowInputFormat.addIterator(job.getConfiguration(), timeFilter);
        }
        // TODO Configure Column Qualifier Regex Filters
    }

    /**
     * @return <code> contextSignature + "_projectedFields" </code>
     */
    private String projectedFieldsName() {
        return contextSignature + "_projectedFields";
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new AccumuloRowInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit ps)
            throws IOException {
        this.reader = reader;
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            if (reader.nextKeyValue()) {
                Text currentKey = reader.getCurrentKey();
                PeekingIterator<Entry<Key, Value>> currentValue = reader.getCurrentValue();

                RowSortedTable<String, String, byte[]> row = TreeBasedTable.create();
                while (currentValue.hasNext()) {
                    Entry<Key, Value> next = currentValue.next();
                    Key key = next.getKey();
                    Value value = next.getValue();
                    row.put(key.getColumnFamily().toString(), key.getColumnQualifier().toString(), value.get());
                }

                int tupleSize = columnInfo.size();
                if (loadRowKey) {
                    tupleSize++;
                }
                Tuple tuple = TupleFactory.getInstance().newTuple(tupleSize);

                int startIndex = 0;
                if (loadRowKey) {
                    tuple.set(0, currentKey.toString());
                    startIndex++;
                }

                for (int i = 0; i < columnInfo.size(); ++i) {
                    int currentIndex = startIndex + i;

                    ColumnInfo cinfo = columnInfo.get(i);
                    if (cinfo.isColumnMap()) {
                        // It's a column Family!
                        Map<String, DataByteArray> cfMap = Maps.newHashMap();
                        Map<String, byte[]> cfResults = row.row(cinfo.getColumnFamily());
                        for (String qualifier : cfResults.keySet()) {
                            // We need to check against the prefix filter to
                            // see if this value should be included. We can't
                            // just rely on the server-side filter, since a
                            // user could specify multiple CF filters for the
                            // same CF.
                            if (cinfo.getColumnPrefix() == null || cinfo.hasPrefixMatch(qualifier)) {
                                cfMap.put(qualifier, new DataByteArray(cfResults.get(qualifier)));
                            }
                            tuple.set(currentIndex, cfMap);
                        }
                    } else {
                        // It's a column so set the value
                        byte[] cell = row.get(cinfo.getColumnFamily(), cinfo.getColumnName());
                        DataByteArray value = cell == null ? null : new DataByteArray(cell);
                        tuple.set(currentIndex, value);
                    }
                }
                return tuple;
            }

            return null;
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<OperatorSet> getFeatures() {
        return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
    }

    @Override
    public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList) throws FrontendException {
        // (row key is not a real column)
        int colOffset = loadRowKey ? 1 : 0;

        if (requiredFieldList == null) {
            throw new FrontendException("RequiredFieldList is null");
        }

        if (requiredFieldList.getFields().size()
                > (columnInfo.size() + colOffset)) {
            throw new FrontendException("The list of columns to project from Accumulo"
                    + " is larger than AccumuloStorage is configured to load.");
        }

        List<RequiredField> requiredFields = requiredFieldList.getFields();
        List<ColumnInfo> newColumns = Lists.newArrayListWithExpectedSize(
                requiredFields.size());

        if (this.requiredFieldList != null) {
            // in addition to PIG, this is also called by this.setLocation().
            LOG.debug("projection is already set. skipping.");
            return new RequiredFieldResponse(true);
        }

        /* How projection is handled :
         *  - pushProjection() is invoked by PIG on the front end
         *  - pushProjection here both stores serialized projection in the
         *    context and adjusts columnInfo_.
         *  - setLocation() is invoked on the backend and it reads the
         *    projection from context. setLocation invokes this method again
         *    so that columnInfo_ is adjusted.
         */

        // colOffset is the offset in our columnList that we need to apply
        // to indexes we get from requiredFields

        // projOffset is the offset to the requiredFieldList we need to apply when
        // figuring out which columns to prune. (if key is pruned,
        // we should skip row key's element in this list when trimming colList)
        int projOffset = colOffset;
        this.requiredFieldList = requiredFieldList;


        // remember the projection
        storeProjectedFieldNames(requiredFieldList);

        if (loadRowKey && (requiredFields.size() < 1
                || requiredFields.get(0).getIndex() != 0)) {
            loadRowKey = false;
            projOffset = 0;
        }

        for (int i = projOffset; i < requiredFields.size(); i++) {
            int fieldIndex = requiredFields.get(i).getIndex();
            newColumns.add(columnInfo.get(fieldIndex - colOffset));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("pushProjection After Projection: loadRowKey is " + loadRowKey);
            for (ColumnInfo colInfo : newColumns) {
                LOG.debug("pushProjection -- col: " + colInfo);
            }
        }
        setColumnInfoList(newColumns);
        return new RequiredFieldResponse(true);
    }

    @Override
    public WritableComparable<?> getSplitComparable(InputSplit is)
            throws IOException {
        return new WritableComparable<InputSplit>() {
            RangeInputSplit tsplit = new RangeInputSplit();

            @Override
            public void readFields(DataInput in) throws IOException {
                tsplit.readFields(in);
            }

            @Override
            public void write(DataOutput out) throws IOException {
                tsplit.write(out);
            }

            @Override
            public int compareTo(InputSplit split) {
                Range range = tsplit.getRange();
                Range range2 = ((RangeInputSplit) split).getRange();

                return range.compareTo(range2);
            }
        };
    }

    @Override
    public String relToAbsPathForStoreLocation(String location, Path curDir)
            throws IOException {
        return location;
    }

    @Override
    public OutputFormat getOutputFormat() throws IOException {
        if (this.outputFormat == null) {
            if (this.jobConfInitialized) {
                this.outputFormat = new AccumuloOutputFormat();
            } else {
                throw new IllegalStateException("setStoreLocation has not been called");
            }
        }
        return outputFormat;
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        String tablename = location;
        if (location.startsWith("accumulo://")) {
            tablename = location.substring(11);
        }

        // Get accumulo connection information from job config
        String user = job.getConfiguration().get("accumulo.user");
        byte[] passwd = job.getConfiguration().get("accumulo.passwd").getBytes();
        String instance = job.getConfiguration().get("accumulo.instance");
        String zookeepers = job.getConfiguration().get("accumulo.zookeepers");

        try {
            AccumuloOutputFormat.setZooKeeperInstance(job.getConfiguration(), instance, zookeepers);
            AccumuloOutputFormat.setOutputInfo(job.getConfiguration(), user, passwd, false, tablename);
        } catch (IllegalStateException ex) {
            // Accumulo is dumb!
            // Ignore
        }

        String serializedSchema = getUDFProperties().getProperty(contextSignature + "_schema");
        if (serializedSchema != null) {
            schema = (ResourceSchema) ObjectSerializer.deserialize(serializedSchema);
        }

        initializeJobConfig(job);
    }

    @Override
    public void checkSchema(ResourceSchema rs) throws IOException {
        if (!(caster instanceof LoadStoreCaster)) {
            LOG.error("Caster must implement LoadStoreCaster for writing to Accumulo.");
            throw new IOException("Bad Caster " + caster.getClass());
        }
        schema = rs;
        getUDFProperties().setProperty(contextSignature + "_schema",
                                       ObjectSerializer.serialize(schema));
    }

    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        this.writer = writer;
    }

    @Override
    public void putNext(Tuple tuple) throws IOException {
        final ResourceFieldSchema[] fieldSchemas = (schema == null) ? null : schema.getFields();
        final byte type = (fieldSchemas == null) ? DataType.findType(tuple.get(0)) : fieldSchemas[0].getType();
        final long ts = System.currentTimeMillis();

        final Mutation mutation = createMutation(tuple.get(0), type);
        for (int i = 1; i < tuple.size(); i++) {
            final byte thisType = (fieldSchemas == null) ? DataType.findType(tuple.get(i)) : fieldSchemas[i].getType();
            final ColumnInfo thisCol = columnInfo.get(i - 1);
            if (!thisCol.isColumnMap()) {
                final String cf = thisCol.getColumnFamily();
                final String cq = thisCol.getColumnName();
                final Value value = new Value(objToBytes(tuple.get(i), thisType));
                mutation.put(cf, cq, ts, value);
            } else {
                Map<String, Object> cfMap = (Map<String, Object>) tuple.get(i);
                for (Entry<String, Object> entry : cfMap.entrySet()) {
                    final String cq = entry.getKey();
                    final Object vobject = entry.getValue();
                    Value value = new Value(objToBytes(vobject, DataType.findType(vobject)));
                    mutation.put(thisCol.getColumnFamily(), cq, ts, value);
                }
            }
        }

        try {
            writer.write(null, mutation);
        } catch (InterruptedException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
        this.contextSignature = signature;
    }

    @Override
    public void cleanupOnFailure(String string, Job job) throws IOException {
    }

    private void initializeJobConfig(Job job) {
        Properties udfProps = getUDFProperties();
        Configuration jobConf = job.getConfiguration();
        for (Entry<Object, Object> entry : udfProps.entrySet()) {
            jobConf.set((String) entry.getKey(), (String) entry.getValue());
        }
        this.jobConfInitialized = true;
    }

    private Properties getUDFProperties() {
        return UDFContext.getUDFContext()
                .getUDFProperties(this.getClass(), new String[]{contextSignature});
    }

    /**
     *
     * @param columnList
     * @param delimiter
     * @param ignoreWhitespace
     * @return
     */
    private List<ColumnInfo> parseColumnList(String columnList,
                                             String delimiter) {
        List<ColumnInfo> colInfo = Lists.newArrayList();

        // Default behavior is to allow combinations of spaces and delimiter
        // which defaults to a comma. Setting to not ignore whitespace will
        // include the whitespace in the columns names
        String[] colNames = columnList.split(delimiter);

        for (String colName : colNames) {
            colInfo.add(new ColumnInfo(colName));
        }

        return colInfo;
    }

    private void storeProjectedFieldNames(RequiredFieldList requiredFieldList)
            throws FrontendException {
        try {
            getUDFProperties().setProperty(projectedFieldsName(),
                                           ObjectSerializer.serialize(
                    requiredFieldList));
        } catch (IOException e) {
            throw new FrontendException(e);
        }
    }

    private void setColumnInfoList(List<ColumnInfo> newColumns) {
        this.columnInfo = newColumns;
    }

    @Override
    public void setUDFContextSignature(String signature) {
        this.contextSignature = signature;
    }

    private Mutation createMutation(Object key, byte type) throws IOException {
        return new Mutation(new Text(objToBytes(key, type)));
    }

    @SuppressWarnings("unchecked")
    private byte[] objToBytes(Object o, byte type) throws IOException {
        LoadStoreCaster localCaster = (LoadStoreCaster) this.caster;
        if (o == null) {
            return null;
        }
        switch (type) {
            case DataType.BYTEARRAY:
                return ((DataByteArray) o).get();
            case DataType.BAG:
                return localCaster.toBytes((DataBag) o);
            case DataType.CHARARRAY:
                return localCaster.toBytes((String) o);
            case DataType.DOUBLE:
                return localCaster.toBytes((Double) o);
            case DataType.FLOAT:
                return localCaster.toBytes((Float) o);
            case DataType.INTEGER:
                return localCaster.toBytes((Integer) o);
            case DataType.LONG:
                return localCaster.toBytes((Long) o);
            // The type conversion here is unchecked.
            // Relying on DataType.findType to do the right thing.
            case DataType.MAP:
                return localCaster.toBytes((Map<String, Object>) o);
            case DataType.NULL:
                return null;
            case DataType.TUPLE:
                return localCaster.toBytes((Tuple) o);
            case DataType.ERROR:
                throw new IOException("Unable to determine type of " + o.getClass());
            default:
                throw new IOException("Unable to find a converter for tuple field " + o);
        }
    }

    /**
     * Class to encapsulate logic around which column names were specified in each position of the column list. Users
     * can specify columns names in one of 4 ways: 'Foo:', 'Foo:*', 'Foo:bar*' or 'Foo:bar'. The first 3 result in a Map
     * being added to the tuple, while the last results in a scalar. The 3rd form results in a prefix-filtered Map.
     */
    public class ColumnInfo {

        final String originalColumnName;  // always set
        final String columnFamily; // always set
        final String columnName; // set if it exists and doesn't contain '*'
        final String columnPrefix; // set if contains a prefix followed by '*'

        public ColumnInfo(String colName) {
            originalColumnName = colName;
            String[] cfAndColumn = colName.split(COLON, 2);

            //CFs are byte[1] and columns are byte[2]
            columnFamily = cfAndColumn[0];
            if (cfAndColumn.length > 1
                    && cfAndColumn[1].length() > 0 && !ASTERISK.equals(cfAndColumn[1])) {
                if (cfAndColumn[1].endsWith(ASTERISK)) {
                    columnPrefix = cfAndColumn[1].substring(
                            0, cfAndColumn[1].length() - 1);
                    columnName = null;
                } else {
                    columnName = cfAndColumn[1];
                    columnPrefix = null;
                }
            } else {
                columnPrefix = null;
                columnName = null;
            }
        }

        public String getColumnFamily() {
            return columnFamily;
        }

        public String getColumnName() {
            return columnName;
        }

        public String getColumnPrefix() {
            return columnPrefix;
        }

        /**
         * Returns whether there is a Column Qualifier or not.
         *
         * @return true if exporting the entire column family
         */
        public boolean isColumnMap() {
            return columnName == null;
        }

        public boolean hasPrefixMatch(String qualifier) {
            return qualifier.startsWith(columnPrefix);
        }

        @Override
        public String toString() {
            return originalColumnName;
        }
    }
}
