package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.cql.EmptyColumnDefinitions;
import java.util.Collections;
import java.util.concurrent.CompletionStage;

/**
 * EmptyAsyncResultSet implementation to be used only for metadata attempt where no cql query is
 * run.
 */
public class EmptyAsyncResultSet implements AsyncResultSet {
  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return EmptyColumnDefinitions.INSTANCE;
  }

  @Override
  public ExecutionInfo getExecutionInfo() {
    return null;
  }

  @Override
  public Iterable<Row> currentPage() {
    return Collections.emptyList();
  }

  @Override
  public int remaining() {
    return 0;
  }

  @Override
  public boolean hasMorePages() {
    return false;
  }

  @Override
  public CompletionStage<AsyncResultSet> fetchNextPage() throws IllegalStateException {
    throw new IllegalStateException(
        "No next page. Use #hasMorePages before calling this method to avoid this error.");
  }

  @Override
  public boolean wasApplied() {
    return true;
  }
}
