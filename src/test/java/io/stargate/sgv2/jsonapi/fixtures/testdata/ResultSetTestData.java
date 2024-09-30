package io.stargate.sgv2.jsonapi.fixtures.testdata;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.cql.EmptyColumnDefinitions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collections;
import java.util.concurrent.CompletionStage;

public class ResultSetTestData extends TestDataSuplier {

  public ResultSetTestData(TestData testData) {
    super(testData);
  }

  public AsyncResultSet emptyResultSet() {
    return new EmptyAsyncResultSet();
  }

  public static class EmptyAsyncResultSet implements AsyncResultSet {
    @NonNull
    @Override
    public ColumnDefinitions getColumnDefinitions() {
      return EmptyColumnDefinitions.INSTANCE;
    }

    @NonNull
    @Override
    public ExecutionInfo getExecutionInfo() {
      return null;
    }

    @NonNull
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

    @NonNull
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
}
