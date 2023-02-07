package io.stargate.sgv2.jsonapi.service.sequencer.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.jsonapi.service.bridge.executor.ReactiveQueryExecutor;
import io.stargate.sgv2.jsonapi.service.sequencer.QueryOptions;
import io.stargate.sgv2.jsonapi.service.sequencer.SingleQuerySequence;

/**
 * Represents start of the query sequence using a single query as source.
 *
 * @param query Query to execute
 * @param options Query options
 * @param handler A handler to handle query execution.
 * @param <OUT> Output from this sequence part.
 */
public record SingleQuerySource<OUT>(
    QueryOuterClass.Query query, QueryOptions options, SingleQuerySequence.Handler<OUT> handler)
    implements SingleQuerySequence<OUT> {

  /** {@inheritDoc} */
  @Override
  public <T> SingleQuerySequence<T> withHandler(Handler<T> handler) {
    return new SingleQuerySource<>(query, options, handler);
  }

  /** {@inheritDoc} */
  @Override
  public Uni<OUT> execute(ReactiveQueryExecutor queryExecutor) {
    return queryExecutor.execute(this);
  }
}
