package io.stargate.sgv2.jsonapi.service.sequencer;

import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.sequencer.impl.CommandResultSink;
import io.stargate.sgv2.jsonapi.service.sequencer.impl.EmptyQuerySequence;
import io.stargate.sgv2.jsonapi.service.sequencer.impl.FunctionalQueryPipe;
import io.stargate.sgv2.jsonapi.service.sequencer.impl.FunctionalSink;
import io.stargate.sgv2.jsonapi.service.sequencer.impl.MultiQueryPipe;
import io.stargate.sgv2.jsonapi.service.sequencer.impl.MultiQuerySource;
import io.stargate.sgv2.jsonapi.service.sequencer.impl.SingleQueryPipe;
import io.stargate.sgv2.jsonapi.service.sequencer.impl.SingleQuerySource;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public interface QuerySequence<OUT> extends ReactiveQuerySequenceExecutor<OUT> {

  //////////////////////
  // Static utilities //
  //////////////////////

  /** @return Returns empty query sequence. */
  static QuerySequence<Void> empty() {
    return new EmptyQuerySequence();
  }

  /**
   * Creates a new query sequence by executing one query.
   *
   * @param query Query to execute
   * @param type Query type
   * @return SingleQuerySequence<QueryOuterClass.ResultSet>
   */
  static SingleQuerySequence<QueryOuterClass.ResultSet> query(
      QueryOuterClass.Query query, QueryOptions.Type type) {
    QueryOptions options = new QueryOptions(type);
    return query(query, options);
  }

  /**
   * Creates a new query sequence by executing one query.
   *
   * @param query Query to execute
   * @param options Query options
   * @return SingleQuerySequence<QueryOuterClass.ResultSet>
   */
  static SingleQuerySequence<QueryOuterClass.ResultSet> query(
      QueryOuterClass.Query query, QueryOptions options) {
    return query(query, options, SingleQuerySequence.DEFAULT_HANDLER);
  }

  /**
   * Creates a new query sequence by executing one query. Uses custom handler.
   *
   * @param query Query to execute
   * @param options Query options
   * @param handler Handler for a query
   * @return SingleQuerySequence<T>
   * @param <T> output of the query execution
   */
  static <T> SingleQuerySequence<T> query(
      QueryOuterClass.Query query, QueryOptions options, SingleQuerySequence.Handler<T> handler) {
    return new SingleQuerySource<>(query, options, handler);
  }

  /**
   * Creates a new query sequence by executing multiple queries.
   *
   * @param queries Queries to execute
   * @param type Type of the query (applies to all)
   * @return MultiQuerySource<QueryOuterClass.ResultSet>
   */
  static MultiQuerySource<QueryOuterClass.ResultSet> queries(
      List<QueryOuterClass.Query> queries, QueryOptions.Type type) {
    QueryOptions options = new QueryOptions(type);
    return queries(queries, options);
  }

  /**
   * Creates a new query sequence by executing multiple queries.
   *
   * @param queries Queries to execute
   * @param options Options for a query (applies to all)
   * @return MultiQuerySource<QueryOuterClass.ResultSet>
   */
  static MultiQuerySource<QueryOuterClass.ResultSet> queries(
      List<QueryOuterClass.Query> queries, QueryOptions options) {
    return queries(queries, options, MultiQuerySequence.DEFAULT_HANDLER);
  }

  /**
   * Creates a new query sequence by executing multiple queries. Uses custom handler.
   *
   * @param queries Queries to execute
   * @param options Options for a query (applies to all)
   * @param handler Handler for queries
   * @return MultiQuerySource<T>
   * @param <T> output of each query execution
   */
  static <T> MultiQuerySource<T> queries(
      List<QueryOuterClass.Query> queries,
      QueryOptions options,
      MultiQuerySequence.Handler<T> handler) {
    return new MultiQuerySource<>(queries, options, handler);
  }

  ///////////////////////
  // Interface methods //
  ///////////////////////

  /** @return Self, used for more readble construction spec. */
  default QuerySequence<OUT> then() {
    return this;
  }

  /**
   * Adds a new query sequence part that executes a single query.
   *
   * @param mapper The mapper that maps output of the previous step to the query to execute
   * @param type Type of the query
   * @return SingleQuerySequence<QueryOuterClass.ResultSet>
   */
  default SingleQuerySequence<QueryOuterClass.ResultSet> query(
      Function<OUT, QueryOuterClass.Query> mapper, QueryOptions.Type type) {
    QueryOptions options = new QueryOptions(type);
    return query(mapper, options);
  }

  /**
   * Adds a new query sequence part that executes a single query.
   *
   * @param mapper The mapper that maps output of the previous step to the query to execute
   * @param options Query options
   * @return SingleQuerySequence<QueryOuterClass.ResultSet>
   */
  default SingleQuerySequence<QueryOuterClass.ResultSet> query(
      Function<OUT, QueryOuterClass.Query> mapper, QueryOptions options) {
    return query(mapper, options, SingleQuerySequence.DEFAULT_HANDLER);
  }

  /**
   * Adds a new query sequence part that executes a single query. Uses custom handler.
   *
   * @param mapper The mapper that maps output of the previous step to the query to execute
   * @param options Query options
   * @param handler Handler for a query
   * @return SingleQuerySequence<T>
   * @param <T> output of the query execution
   */
  default <T> SingleQuerySequence<T> query(
      Function<OUT, QueryOuterClass.Query> mapper,
      QueryOptions options,
      SingleQuerySequence.Handler<T> handler) {
    return new SingleQueryPipe<>(this, mapper, options, handler);
  }

  /**
   * Adds a new query sequence part that executes multiple query.
   *
   * @param mapper The mapper that maps output of the previous step to queries to execute
   * @param type Type of the query (applies to all)
   * @return MultiQuerySource<QueryOuterClass.ResultSet>
   */
  default MultiQuerySequence<QueryOuterClass.ResultSet> queries(
      Function<OUT, List<QueryOuterClass.Query>> mapper, QueryOptions.Type type) {
    QueryOptions options = new QueryOptions(type);
    return queries(mapper, options);
  }

  /**
   * Adds a new query sequence part that executes multiple query.
   *
   * @param mapper The mapper that maps output of the previous step to queries to execute
   * @param options Options of the query (applies to all)
   * @return MultiQuerySource<QueryOuterClass.ResultSet>
   */
  default MultiQuerySequence<QueryOuterClass.ResultSet> queries(
      Function<OUT, List<QueryOuterClass.Query>> mapper, QueryOptions options) {
    return new MultiQueryPipe<>(this, mapper, options, MultiQuerySequence.DEFAULT_HANDLER);
  }

  /**
   * Adds a new query sequence part that executes multiple query. Uses custom handler.
   *
   * @param mapper The mapper that maps output of the previous step to queries to execute
   * @param options Options for a query (applies to all)
   * @param handler Handler for queries
   * @return MultiQuerySource<T>
   * @param <T> output of each query execution
   */
  default <T> MultiQuerySequence<T> queries(
      Function<OUT, List<QueryOuterClass.Query>> mapper,
      QueryOptions options,
      MultiQuerySequence.Handler<T> handler) {
    return new MultiQueryPipe<>(this, mapper, options, handler);
  }

  /**
   * Pipes this sequence to another.
   *
   * @param pipeFunction Mapping function.
   * @return QuerySequence
   * @param <T> output of created query sequence
   */
  default <T> QuerySequence<T> pipeTo(Function<OUT, QuerySequence<T>> pipeFunction) {
    return new FunctionalQueryPipe<>(this, pipeFunction);
  }

  default QuerySequenceSink<Supplier<CommandResult>> pipeToSink(
      Function<OUT, QuerySequenceSink<Supplier<CommandResult>>> pipeFunction) {
    return new FunctionalSink<>(this, pipeFunction);
  }

  /**
   * Sinks a sequence to the {@link CommandResult} supplier.
   *
   * @param mapper The mapper that maps output of the previous step to the result
   * @return QuerySequenceSink<Supplier<CommandResult>>
   */
  default QuerySequenceSink<Supplier<CommandResult>> sink(
      Function<OUT, Supplier<CommandResult>> mapper) {
    return new CommandResultSink<>(this, mapper);
  }
}
