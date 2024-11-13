package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.RowAccumulator;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import org.apache.commons.lang3.NotImplementedException;

/**
 * Accumulates rows from multiple {@link AsyncResultSet} instances into a single page of rows using
 * a {@link RowAccumulator}.
 *
 * <p>Used when we want to pull rows from multiple result set pages and sort them in memory. Call
 * {@link #accumulate(AsyncResultSet)} everytime you have a result set to add to the accumulator.
 */
public class AccumulatingAsyncResultSet implements AsyncResultSet {

  private final RowAccumulator rowAccumulator;
  private ColumnDefinitions columnDefinitions;

  private int returnedPages = 0;

  public AccumulatingAsyncResultSet(RowAccumulator rowAccumulator) {
    this.rowAccumulator = Objects.requireNonNull(rowAccumulator, "rowAccumulator must not be null");
  }

  /**
   * Adds the rows from the current page to the accumulator.
   *
   * @param resultSet The result set to add to the accumulator.
   */
  public void accumulate(AsyncResultSet resultSet) {
    if (columnDefinitions == null) {
      columnDefinitions = resultSet.getColumnDefinitions();
    }
    resultSet
        .currentPage()
        .forEach(
            row -> {
              if (!rowAccumulator.accumulate(row)) {
                // TODO: change so we are not throwing an application exception here, would be
                // better to
                // return false or throw a checked so the application can generate a better error
                // message
                throw SortException.Code.CANNOT_SORT_TOO_MUCH_DATA.get();
              }
            });
  }

  @Override
  @NonNull
  public ColumnDefinitions getColumnDefinitions() {
    if (columnDefinitions == null) {
      throw new IllegalStateException("No column definitions available");
    }
    return columnDefinitions;
  }

  @Override
  @NonNull
  public ExecutionInfo getExecutionInfo() {
    throw new NotImplementedException();
  }

  @Override
  @NonNull
  public Iterable<Row> currentPage() {
    returnedPages++;
    return rowAccumulator.getPage();
  }

  @Override
  public int remaining() {
    return -1;
  }

  @Override
  public boolean hasMorePages() {
    return returnedPages == 0;
  }

  @Override
  @NonNull
  public CompletionStage<AsyncResultSet> fetchNextPage() throws IllegalStateException {
    throw new NotImplementedException();
  }

  @Override
  public boolean wasApplied() {
    return true;
  }
}
