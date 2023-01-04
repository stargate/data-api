package io.stargate.sgv3.docsapi.service.sequencer.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv3.docsapi.service.bridge.executor.ReactiveQueryExecutor;
import io.stargate.sgv3.docsapi.service.sequencer.MultiQuerySequence;
import io.stargate.sgv3.docsapi.service.sequencer.QueryOptions;
import java.util.List;

/**
 * Represents start of the query sequence using multiple queries as source.
 *
 * @param queries Queries to execute
 * @param options Query options
 * @param handler A handler to handle each query execution.
 * @param <OUT> Output from each query.
 */
public record MultiQuerySource<OUT>(
    List<QueryOuterClass.Query> queries, QueryOptions options, Handler<OUT> handler)
    implements MultiQuerySequence<OUT> {

  /** {@inheritDoc} */
  @Override
  public <T> MultiQuerySequence<T> withHandler(Handler<T> handler) {
    return new MultiQuerySource<>(queries, options, handler);
  }

  /** {@inheritDoc} */
  @Override
  public Uni<List<OUT>> execute(ReactiveQueryExecutor queryExecutor) {
    return queryExecutor.execute(this);
  }
}
