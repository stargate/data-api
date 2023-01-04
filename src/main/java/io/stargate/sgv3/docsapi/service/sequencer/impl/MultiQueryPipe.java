package io.stargate.sgv3.docsapi.service.sequencer.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv3.docsapi.service.bridge.executor.ReactiveQueryExecutor;
import io.stargate.sgv3.docsapi.service.sequencer.MultiQuerySequence;
import io.stargate.sgv3.docsapi.service.sequencer.QueryOptions;
import io.stargate.sgv3.docsapi.service.sequencer.QuerySequence;
import java.util.List;
import java.util.function.Function;

/**
 * Represents the pipe in the query sequence that executes multiple queries.
 *
 * @param source The source, or previous step in the sequence.
 * @param mapper The mapper that maps output of the previous step to the query to execute
 * @param options Query options
 * @param handler A handler to handle each query execution.
 * @param <IN> Input to this sequence part.
 * @param <OUT> Output from each query.
 */
public record MultiQueryPipe<IN, OUT>(
    QuerySequence<IN> source,
    Function<IN, List<QueryOuterClass.Query>> mapper,
    QueryOptions options,
    Handler<OUT> handler)
    implements MultiQuerySequence<OUT> {

  /** {@inheritDoc} */
  @Override
  public <T> MultiQuerySequence<T> withHandler(Handler<T> handler) {
    return new MultiQueryPipe<>(source, mapper, options, handler);
  }

  /** {@inheritDoc} */
  @Override
  public Uni<List<OUT>> execute(ReactiveQueryExecutor queryExecutor) {
    return queryExecutor.execute(this);
  }
}
