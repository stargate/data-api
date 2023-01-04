package io.stargate.sgv3.docsapi.service.sequencer.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv3.docsapi.service.bridge.executor.ReactiveQueryExecutor;
import io.stargate.sgv3.docsapi.service.sequencer.QuerySequence;
import java.util.function.Function;

/**
 * Part of the query sequence that can switch from one sequence to another.
 *
 * @param source The source, or previous step in the sequence.
 * @param function The function that provides nex sequence.
 * @param <IN> Input to this sequence part.
 * @param <OUT> Output from each query.
 */
public record FunctionalQueryPipe<IN, OUT>(
    QuerySequence<IN> source, Function<IN, QuerySequence<OUT>> function)
    implements QuerySequence<OUT> {

  /** {@inheritDoc} */
  @Override
  public Uni<OUT> execute(ReactiveQueryExecutor queryExecutor) {
    return queryExecutor.execute(this);
  }
}
