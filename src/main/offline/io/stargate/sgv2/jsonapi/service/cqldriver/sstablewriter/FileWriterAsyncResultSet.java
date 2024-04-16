package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * A ResultSet implementation that wraps the response from a write operation to a file. This is
 * expected to have only one row with a single column that indicates whether the write operation was
 * successful or not.
 */
public class FileWriterAsyncResultSet implements AsyncResultSet {

  /* The column definitions for the response row */
  private final ColumnDefinitions columnDefs;
  /* The response row */
  private final Row insertResponseRow;

  /**
   * Constructs a new FileWriterAsyncResultSet
   *
   * @param columnDefs The column definitions for the response row
   * @param insertResponseRow The response row
   */
  public FileWriterAsyncResultSet(ColumnDefinitions columnDefs, Row insertResponseRow) {
    this.columnDefs = columnDefs;
    this.insertResponseRow = insertResponseRow;
  }

  /**
   * Getter for the column definitions
   *
   * @return columnDefs The column definitions for the response row
   */
  @NonNull
  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return this.columnDefs;
  }

  /**
   * Getter for the execution info
   *
   * @return ExecutionInfo The execution info
   * @throws UnsupportedOperationException This method is not supported
   */
  @NonNull
  @Override
  public ExecutionInfo getExecutionInfo() {
    throw new UnsupportedOperationException();
  }

  /**
   * Getter for the remaining number of rows
   *
   * @return int The remaining number of rows
   * @throws UnsupportedOperationException This method is not supported
   */
  @Override
  public int remaining() {
    throw new UnsupportedOperationException();
  }

  /**
   * Getter for the response row
   *
   * @return Iterable<Row> The response row(s)
   */
  @NonNull
  @Override
  public Iterable<Row> currentPage() {
    return List.of(insertResponseRow);
  }

  /**
   * Boolean indicating whether there are more pages
   *
   * @return false indicating that there are no more pages
   */
  @Override
  public boolean hasMorePages() {
    return false;
  }

  /**
   * Fetches the next page
   *
   * @return CompletionStage<AsyncResultSet> The next page. This implementation always returns null
   */
  @NonNull
  @Override
  public CompletionStage<AsyncResultSet> fetchNextPage() throws IllegalStateException {
    return null;
  }

  /**
   * Boolean indicating whether the write operation was successful
   *
   * @return boolean indicating whether the write operation was successful
   */
  @Override
  public boolean wasApplied() {
    return insertResponseRow.getBoolean("[applied]");
  }
}
