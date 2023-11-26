/*
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
package io.stargate.sgv2.jsonapi.service.testutil;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import java.nio.ByteBuffer;
import java.util.List;

public class MockRow implements Row {
  private final CodecRegistry DEFAULT_CODECS = new DefaultCodecRegistry("json-api-test");
  private final ColumnDefinitions columnDefs;
  private final int index;
  private final List<ByteBuffer> values;

  public MockRow(ColumnDefinitions columnDefs, int index, List<ByteBuffer> values) {
    this.columnDefs = columnDefs;
    this.index = index;
    this.values = values;
  }

  @Override
  public int size() {
    return columnDefs.size();
  }

  @Override
  public CodecRegistry codecRegistry() {
    return DEFAULT_CODECS;
  }

  @Override
  public ProtocolVersion protocolVersion() {
    return DefaultProtocolVersion.V5;
  }

  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return columnDefs;
  }

  @Override
  public List<Integer> allIndicesOf(String name) {
    return columnDefs.allIndicesOf(name);
  }

  @Override
  public int firstIndexOf(String name) {
    return columnDefs.firstIndexOf(name);
  }

  @Override
  public List<Integer> allIndicesOf(CqlIdentifier id) {
    return columnDefs.allIndicesOf(id);
  }

  @Override
  public int firstIndexOf(CqlIdentifier id) {
    return columnDefs.firstIndexOf(id);
  }

  @Override
  public DataType getType(int i) {
    return columnDefs.get(i).getType();
  }

  @Override
  public DataType getType(String name) {
    return columnDefs.get(name).getType();
  }

  @Override
  public DataType getType(CqlIdentifier id) {
    return columnDefs.get(id).getType();
  }

  @Override
  public ByteBuffer getBytesUnsafe(int i) {
    return values.get(i);
  }

  @Override
  public boolean isDetached() {
    return true;
  }

  @Override
  public void attach(AttachmentPoint attachmentPoint) {}

  // equals and hashCode required for TCK tests that check that two subscribers
  // receive the exact same set of items.

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MockRow)) {
      return false;
    }
    MockRow mockRow = (MockRow) o;
    return index == mockRow.index;
  }

  @Override
  public int hashCode() {
    return index;
  }
}
