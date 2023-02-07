package io.stargate.sgv2.jsonapi.service.sequencer;

/**
 * Marker for all QuerySequence part that can accept query options.
 *
 * @param <OUT>
 */
public interface QuerySequenceWithOptions<OUT> extends QuerySequence<OUT> {

  /** @return Returns query execution options for this sequence part. */
  QueryOptions options();

  /**
   * Sets page size of this sequence part.
   *
   * @param pageSize Page size to be set in the query parameters.
   * @return Self
   * @see QueryOptions#setPageSize(Integer)
   */
  default QuerySequenceWithOptions<OUT> withPageSize(int pageSize) {
    options().setPageSize(pageSize);
    return this;
  }

  /**
   * Sets page state of this sequence part.
   *
   * @param pagingState Page state to be set in the query parameters.
   * @return Self
   * @see QueryOptions#setPagingState(String)
   */
  default QuerySequenceWithOptions<OUT> withPagingState(String pagingState) {
    options().setPagingState(pagingState);
    return this;
  }
}
