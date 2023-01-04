package io.stargate.sgv3.docsapi.service.sequencer;

import io.stargate.bridge.proto.QueryOuterClass;
import java.util.List;

/**
 * Interface for a query sequence that executes multiple queries.
 *
 * @param <OUT> Type that is the outcome of this sequence part.
 */
public interface MultiQuerySequence<OUT> extends QuerySequenceWithOptions<List<OUT>> {

  /** Default handler, maps to the result itself, and (re-)throws throwable if any. */
  Handler<QueryOuterClass.ResultSet> DEFAULT_HANDLER =
      (result, throwable, i) -> {
        if (null != throwable) {
          throw throwable;
        }
        return result;
      };

  /** {@inheritDoc} */
  @Override
  default MultiQuerySequence<OUT> withPageSize(int pageSize) {
    QuerySequenceWithOptions.super.withPageSize(pageSize);
    return this;
  }

  /** {@inheritDoc} */
  @Override
  default MultiQuerySequence<OUT> withPagingState(String pagingState) {
    QuerySequenceWithOptions.super.withPagingState(pagingState);
    return this;
  }

  /** @return Handler for this query sequence. */
  Handler<OUT> handler();

  /**
   * Updates the handler of this query sequence.
   *
   * @param handler New handler.
   * @return Updated instance.
   */
  <T> MultiQuerySequence<T> withHandler(Handler<T> handler);

  /**
   * Query result handler.
   *
   * @param <OUT> Output of the handler.
   */
  @FunctionalInterface
  interface Handler<OUT> {

    /**
     * Handles result of a single query execution, or a throwable.
     *
     * @param result Result of the query execution, can be <code>null</code>.
     * @param throwable Throwable being thrown during the execution of the query.
     * @param index Index of the query to handle.
     * @return Result of the handler.
     */
    OUT handle(QueryOuterClass.ResultSet result, Throwable throwable, int index) throws Throwable;
  }
}
