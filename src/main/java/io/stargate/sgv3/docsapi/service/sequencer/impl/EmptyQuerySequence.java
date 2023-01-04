package io.stargate.sgv3.docsapi.service.sequencer.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv3.docsapi.service.bridge.executor.ReactiveQueryExecutor;
import io.stargate.sgv3.docsapi.service.sequencer.QuerySequence;

/** Represents empty query sequence, started from no query. */
public record EmptyQuerySequence() implements QuerySequence<Void> {

  /** {@inheritDoc} */
  @Override
  public Uni<Void> execute(ReactiveQueryExecutor queryExecutor) {
    return Uni.createFrom().voidItem();
  }
}
