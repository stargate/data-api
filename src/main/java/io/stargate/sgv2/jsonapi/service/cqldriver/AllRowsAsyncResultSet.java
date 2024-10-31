package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.service.operation.ReadAttempt;
import java.util.concurrent.CompletionStage;

/**
 * AllRowsAsyncResultSet implementation to return {@link Row} for multipage reads in {@link
 * ReadAttempt} This is used in memory sorting.
 */
public class AllRowsAsyncResultSet implements AsyncResultSet {
  private final RowsContainer rowsContainer;
  private ColumnDefinitions columnDefinitions;

  public AllRowsAsyncResultSet(RowsContainer rowsContainer) {
    this.rowsContainer = rowsContainer;
  }

  public void add(AsyncResultSet resultSet) {
    resultSet
        .currentPage()
        .forEach(
            row -> {
              if (!rowsContainer.add(row)) {
                throw SortException.Code.CANNOT_SORT_TOO_MUCH_DATA.get();
              }
            });
  }

  public void addColumDefinitions(ColumnDefinitions columnDefinitions) {
    this.columnDefinitions = columnDefinitions;
  }

  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return columnDefinitions;
  }

  @Override
  public ExecutionInfo getExecutionInfo() {
    return null;
  }

  @Override
  public Iterable<Row> currentPage() {
    return rowsContainer.getRequiredPage();
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
