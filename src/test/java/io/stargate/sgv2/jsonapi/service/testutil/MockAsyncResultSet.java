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

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MockAsyncResultSet implements AsyncResultSet {

  private final List<Row> rows;
  private final Iterator<Row> iterator;
  private final CompletionStage<AsyncResultSet> nextPage;
  private final ExecutionInfo executionInfo = mock(ExecutionInfo.class);
  private final ColumnDefinitions columnDefs;
  private int remaining;

  public MockAsyncResultSet(
      ColumnDefinitions columnDefs, int size, CompletionStage<AsyncResultSet> nextPage) {
    this(
        columnDefs,
        IntStream.range(0, size)
            .boxed()
            .map(id -> new MockRow(columnDefs, id))
            .collect(Collectors.toList()),
        nextPage);
  }

  public MockAsyncResultSet(
      ColumnDefinitions columnDefs, List<Row> rows, CompletionStage<AsyncResultSet> nextPage) {
    this.columnDefs = columnDefs;
    this.rows = rows;
    iterator = rows.iterator();
    remaining = rows.size();
    this.nextPage = nextPage;
  }

  @Override
  public Row one() {
    Row next = iterator.next();
    remaining--;
    return next;
  }

  @Override
  public int remaining() {
    return remaining;
  }

  @Override
  public List<Row> currentPage() {
    return new ArrayList<>(rows);
  }

  @Override
  public boolean hasMorePages() {
    return nextPage != null;
  }

  @Override
  public CompletionStage<AsyncResultSet> fetchNextPage() throws IllegalStateException {
    return nextPage;
  }

  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return columnDefs;
  }

  @Override
  public ExecutionInfo getExecutionInfo() {
    return executionInfo;
  }

  @Override
  public boolean wasApplied() {
    return true;
  }
}
