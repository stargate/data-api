package io.stargate.sgv2.jsonapi.service.sequencer;

import io.stargate.bridge.proto.QueryOuterClass;

/**
 * Interface for a query sequence that executes a single query.
 *
 * @param <OUT> Type that is the outcome of this sequence part.
 */
public interface SingleQuerySequence<OUT> extends QuerySequenceWithOptions<OUT> {

  /** Default handler, maps to the result itself, and (re-)throws throwable if any. */
  Handler<QueryOuterClass.ResultSet> DEFAULT_HANDLER =
      (result, throwable) -> {
        if (null != throwable) {
          throw throwable;
        }
        return result;
      };

  /** {@inheritDoc} */
  @Override
  default SingleQuerySequence<OUT> withPageSize(int pageSize) {
    QuerySequenceWithOptions.super.withPageSize(pageSize);
    return this;
  }

  /** {@inheritDoc} */
  @Override
  default SingleQuerySequence<OUT> withPagingState(String pagingState) {
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
  <T> SingleQuerySequence<T> withHandler(SingleQuerySequence.Handler<T> handler);

  /**
   * Query result handler.
   *
   * @param <OUT> Output of the handler.
   */
  @FunctionalInterface
  interface Handler<OUT> {

    /**
     * Handles result of the query execution, or a throwable.
     *
     * @param result Result of the query execution, can be <code>null</code>.
     * @param throwable Throwable being thrown during the execution of the query.
     * @return Result of the handler.
     */
    OUT handle(QueryOuterClass.ResultSet result, Throwable throwable) throws Throwable;
  }
}
