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

import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.cql.EmptyColumnDefinitions;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class MockRow implements Row {

  private int index;

  MockRow(int index) {
    this.index = index;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public CodecRegistry codecRegistry() {
    return mock(CodecRegistry.class);
  }

  @Override
  public ProtocolVersion protocolVersion() {
    return DefaultProtocolVersion.V4;
  }

  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return EmptyColumnDefinitions.INSTANCE;
  }

  @Override
  public List<Integer> allIndicesOf(String name) {
    return Collections.singletonList(0);
  }

  @Override
  public int firstIndexOf(String name) {
    return 0;
  }

  @Override
  public List<Integer> allIndicesOf(CqlIdentifier id) {
    return Collections.singletonList(0);
  }

  @Override
  public int firstIndexOf(CqlIdentifier id) {
    return 0;
  }

  @Override
  public DataType getType(int i) {
    return DataTypes.INT;
  }

  @Override
  public DataType getType(String name) {
    return DataTypes.INT;
  }

  @Override
  public DataType getType(CqlIdentifier id) {
    return DataTypes.INT;
  }

  @Override
  public ByteBuffer getBytesUnsafe(int i) {
    return null;
  }

  @Override
  public boolean isDetached() {
    return false;
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
