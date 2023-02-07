package io.stargate.sgv2.jsonapi.service.sequencer.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.jsonapi.service.bridge.executor.ReactiveQueryExecutor;
import io.stargate.sgv2.jsonapi.service.sequencer.QueryOptions;
import io.stargate.sgv2.jsonapi.service.sequencer.QuerySequence;
import io.stargate.sgv2.jsonapi.service.sequencer.SingleQuerySequence;
import java.util.function.Function;

/**
 * Represents the pipe in the query sequence that executes a single query.
 *
 * @param source The source, or previous step in the sequence.
 * @param mapper The mapper that maps output of the previous step to the query to execute
 * @param options Query options
 * @param handler A handler to handle query execution.
 * @param <IN> Input to this sequence part.
 * @param <OUT> Output from this sequence part.
 */
public record SingleQueryPipe<IN, OUT>(
    QuerySequence<IN> source,
    Function<IN, QueryOuterClass.Query> mapper,
    QueryOptions options,
    SingleQuerySequence.Handler<OUT> handler)
    implements SingleQuerySequence<OUT> {

  /** {@inheritDoc} */
  @Override
  public <T> SingleQuerySequence<T> withHandler(Handler<T> handler) {
    return new SingleQueryPipe<>(source, mapper, options, handler);
  }

  /** {@inheritDoc} */
  @Override
  public Uni<OUT> execute(ReactiveQueryExecutor queryExecutor) {
    return queryExecutor.execute(this);
  }
}
