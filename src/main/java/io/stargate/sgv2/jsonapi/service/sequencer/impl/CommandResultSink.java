package io.stargate.sgv2.jsonapi.service.sequencer.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.bridge.executor.ReactiveQueryExecutor;
import io.stargate.sgv2.jsonapi.service.sequencer.QuerySequence;
import io.stargate.sgv2.jsonapi.service.sequencer.QuerySequenceSink;
import io.stargate.sgv2.jsonapi.service.sequencer.ReactiveQuerySequenceExecutor;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Basic sink that maps the input to the {@link CommandResult} supplier.
 *
 * @param source The source, or previous step in the sequence.
 * @param mapper The mapper that maps output of the previous step to the query to execute
 * @param <IN> Input to this sequence part.
 */
public record CommandResultSink<IN>(
    QuerySequence<IN> source, Function<IN, Supplier<CommandResult>> mapper)
    implements ReactiveQuerySequenceExecutor<Supplier<CommandResult>>,
        QuerySequenceSink<Supplier<CommandResult>> {

  /** {@inheritDoc} */
  @Override
  public Uni<Supplier<CommandResult>> execute(ReactiveQueryExecutor queryExecutor) {
    return queryExecutor.execute(this);
  }

  /** {@inheritDoc} */
  @Override
  public ReactiveQuerySequenceExecutor<Supplier<CommandResult>> reactive() {
    return this;
  }
}
