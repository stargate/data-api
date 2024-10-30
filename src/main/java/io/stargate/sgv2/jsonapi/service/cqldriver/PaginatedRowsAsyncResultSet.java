package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import io.stargate.sgv2.jsonapi.service.operation.SortableReadAttempt;
import java.util.concurrent.CompletionStage;

/**
 * AsyncResultSet implementation to be used only for in memory sort in the {@link
 * SortableReadAttempt} attempt where no cql query is run.
 */
public class PaginatedRowsAsyncResultSet implements AsyncResultSet {
  private final ResultRowContainer container;
  private ColumnDefinitions columnDefinitions;

  /*
   * Constructor to initialize the SortedRowsAsyncResultSet with skip, limit, errorLimit and
   * comparator for sorting rows.
   * @param skip - number of rows to skip
   * @param limit - maximum number of rows to return
   * @param errorLimit - If more rows than the errorLimit is read, error out
   * @param comparator - comparator for sorting rows
   */
  public PaginatedRowsAsyncResultSet(ResultRowContainer container) {
    this.container = container;
  }

  public void add(AsyncResultSet resultSet) {
    resultSet
        .currentPage()
        .forEach(
            row -> {
              if (!container.add(row)) {
                throw new IllegalStateException("Error adding row to container");
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
    return container.getRequiredPage();
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
