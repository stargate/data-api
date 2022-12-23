package io.stargate.sgv3.docsapi.service.bridge.executor;

import com.google.protobuf.BytesValue;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * Utility used by the {@link QueryExecutor} in order to track the state for executing the same
 * query in the paginated mode.
 */
@Value.Immutable
public interface QueryState {

  /** @return Current page size to be used with the query. */
  @Value.Parameter
  int pageSize();

  /**
   * @return Current paging state to be used with the query. Can be <code>null</code> to denote no
   *     paging state exists.
   */
  @Value.Parameter
  @Nullable
  BytesValue pagingState();

  /**
   * Constructs the next state for the query.
   *
   * @param nextPagingState Updated paging state, usually received from the result set.
   * @param exponentPageSize If current page size should be increased exponentially.
   * @param maxPageSize The absolute max page size that should never be exceeded.
   * @return Next {@link QueryState}.
   */
  default QueryState next(BytesValue nextPagingState, boolean exponentPageSize, int maxPageSize) {
    int nextPageSize = exponentPageSize ? Math.min(pageSize() * 2, maxPageSize) : pageSize();

    return ImmutableQueryState.of(nextPageSize, nextPagingState);
  }
}
