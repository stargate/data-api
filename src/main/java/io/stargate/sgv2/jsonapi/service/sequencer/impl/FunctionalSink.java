package io.stargate.sgv2.jsonapi.service.sequencer.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.service.bridge.executor.ReactiveQueryExecutor;
import io.stargate.sgv2.jsonapi.service.sequencer.QuerySequence;
import io.stargate.sgv2.jsonapi.service.sequencer.QuerySequenceSink;
import io.stargate.sgv2.jsonapi.service.sequencer.ReactiveQuerySequenceExecutor;
import java.util.function.Function;

/**
 * Part of the query sequence that can switch from one sequence to another sink.
 *
 * @param source The source, or previous step in the sequence.
 * @param function The function that provides the next sink.
 * @param <IN> Input to this sequence part.
 * @param <OUT> Output from each query.
 */
public record FunctionalSink<IN, OUT>(
    QuerySequence<IN> source, Function<IN, QuerySequenceSink<OUT>> function)
    implements ReactiveQuerySequenceExecutor<OUT>, QuerySequenceSink<OUT> {

  @Override
  public Uni<OUT> execute(ReactiveQueryExecutor queryExecutor) {
    return queryExecutor.execute(this);
  }

  @Override
  public ReactiveQuerySequenceExecutor<OUT> reactive() {
    return this;
  }
}
