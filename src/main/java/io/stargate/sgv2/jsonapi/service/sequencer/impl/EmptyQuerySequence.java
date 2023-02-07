package io.stargate.sgv2.jsonapi.service.sequencer.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.service.bridge.executor.ReactiveQueryExecutor;
import io.stargate.sgv2.jsonapi.service.sequencer.QuerySequence;

/** Represents empty query sequence, started from no query. */
public record EmptyQuerySequence() implements QuerySequence<Void> {

  /** {@inheritDoc} */
  @Override
  public Uni<Void> execute(ReactiveQueryExecutor queryExecutor) {
    return Uni.createFrom().voidItem();
  }
}
