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

import java.io.IOException;
import java.util.Map;

import org.apache.pig.LoadCaster;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

/**
 *
 * @author pgollakota
 */
public class AccumuloBinaryConverter implements LoadCaster {

    @Override
    public Long bytesToLong(byte[] bytes) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Float bytesToFloat(byte[] bytes) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Double bytesToDouble(byte[] bytes) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Integer bytesToInteger(byte[] bytes) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String bytesToCharArray(byte[] bytes) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, Object> bytesToMap(byte[] bytes) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Tuple bytesToTuple(byte[] bytes, ResourceFieldSchema rfs)
            throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public DataBag bytesToBag(byte[] bytes, ResourceFieldSchema rfs)
            throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
