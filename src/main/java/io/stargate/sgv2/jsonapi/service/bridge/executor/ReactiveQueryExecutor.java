package io.stargate.sgv2.jsonapi.service.bridge.executor;

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Int32Value;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.config.QueriesConfig;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.config.DocumentConfig;
import io.stargate.sgv2.jsonapi.service.sequencer.MultiQuerySequence;
import io.stargate.sgv2.jsonapi.service.sequencer.QueryOptions;
import io.stargate.sgv2.jsonapi.service.sequencer.QuerySequence;
import io.stargate.sgv2.jsonapi.service.sequencer.QuerySequenceSink;
import io.stargate.sgv2.jsonapi.service.sequencer.SingleQuerySequence;
import io.stargate.sgv2.jsonapi.service.sequencer.impl.CommandResultSink;
import io.stargate.sgv2.jsonapi.service.sequencer.impl.FunctionalQueryPipe;
import io.stargate.sgv2.jsonapi.service.sequencer.impl.FunctionalSink;
import io.stargate.sgv2.jsonapi.service.sequencer.impl.MultiQueryPipe;
import io.stargate.sgv2.jsonapi.service.sequencer.impl.MultiQuerySource;
import io.stargate.sgv2.jsonapi.service.sequencer.impl.SingleQueryPipe;
import io.stargate.sgv2.jsonapi.service.sequencer.impl.SingleQuerySource;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.function.Supplier;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/**
 * Reactive implementation of the query executor that knows how to execute all {@link QuerySequence}
 * types.
 */
@ApplicationScoped
public class ReactiveQueryExecutor {

  private final QueriesConfig queriesConfig;

  private final DocumentConfig documentConfig;

  private final StargateRequestInfo stargateRequestInfo;

  @Inject
  public ReactiveQueryExecutor(
      QueriesConfig queriesConfig,
      DocumentConfig documentConfig,
      StargateRequestInfo stargateRequestInfo) {
    this.queriesConfig = queriesConfig;
    this.documentConfig = documentConfig;
    this.stargateRequestInfo = stargateRequestInfo;
  }

  /**
   * Executes {@link SingleQuerySource}.
   *
   * @param source Input
   * @return Uni emitting the output
   * @param <OUT> Type that source outputs
   */
  public <OUT> Uni<OUT> execute(SingleQuerySource<OUT> source) {
    return executeWithOptions(source.query(), source.options(), source.handler());
  }

  /**
   * Executes {@link SingleQueryPipe}.
   *
   * @param pipe Pipe
   * @return Uni emitting the output
   * @param <IN> Type that is input to a pipe
   * @param <OUT> Type that pipe outputs
   */
  public <IN, OUT> Uni<OUT> execute(SingleQueryPipe<IN, OUT> pipe) {
    // call source execution
    return pipe.source()
        .execute(this)

        // map using mapper to get nex query
        .map(pipe.mapper())

        // execute query
        .flatMap(query -> executeWithOptions(query, pipe.options(), pipe.handler()));
  }

  /**
   * Executes {@link MultiQuerySource}.
   *
   * @param source Input
   * @return Uni emitting the list of outputs
   * @param <OUT> Type that source outputs
   */
  public <OUT> Uni<List<OUT>> execute(MultiQuerySource<OUT> source) {
    return executeWithOptions(source.queries(), source.options(), source.handler());
  }

  /**
   * Executes {@link MultiQueryPipe}.
   *
   * @param pipe Pipe
   * @return Uni emitting the list of outputs
   * @param <IN> Type that is input to a pipe
   * @param <OUT> Type that pipe outputs
   */
  public <IN, OUT> Uni<List<OUT>> execute(MultiQueryPipe<IN, OUT> pipe) {
    // call source execution
    return pipe.source()
        .execute(this)

        // map using mapper to get nex query
        .map(pipe.mapper())

        // execute query
        .flatMap(queries -> executeWithOptions(queries, pipe.options(), pipe.handler()));
  }

  /**
   * Executes {@link FunctionalQueryPipe}.
   *
   * @param pipe Pipe
   * @return Uni emitting the output
   * @param <IN> Type that is input to a pipe
   * @param <OUT> Type that pipe outputs
   */
  public <IN, OUT> Uni<OUT> execute(FunctionalQueryPipe<IN, OUT> pipe) {

    // execute source
    return pipe.source()
        .execute(this)

        // then use function to get next sequence
        // and execute that one as well
        .flatMap(
            in -> {
              QuerySequence<OUT> sequence = pipe.function().apply(in);
              return sequence.execute(this);
            });
  }

  /**
   * Executes {@link FunctionalSink}.
   *
   * @param sink Sink
   * @return Uni emitting the output
   * @param <IN> Type that is input to the sink
   * @param <OUT> Type that sink produces
   */
  public <IN, OUT> Uni<OUT> execute(FunctionalSink<IN, OUT> sink) {

    // start from source
    return sink.source()
        .execute(this)

        // map to other sink and execute
        .flatMap(
            in -> {
              QuerySequenceSink<OUT> next = sink.function().apply(in);
              return next.reactive().execute(this);
            });
  }

  /**
   * Executes {@link CommandResultSink}.
   *
   * @param sink Sink
   * @return Uni emitting the {@link CommandResult} supplier
   * @param <IN> Type that is input to the sink
   */
  public <IN> Uni<Supplier<CommandResult>> execute(CommandResultSink<IN> sink) {
    return sink.source().execute(this).map(sink.mapper());
  }

  // internal implementation of the execute multiple query
  private <OUT> Uni<List<OUT>> executeWithOptions(
      List<QueryOuterClass.Query> queries,
      QueryOptions options,
      MultiQuerySequence.Handler<OUT> handler) {

    List<Uni<OUT>> unis = new ArrayList<>(queries.size());
    for (int i = 0; i < queries.size(); i++) {
      QueryOuterClass.Query query = queries.get(i);
      final int index = i;
      Uni<OUT> uni = executeWithOptions(query, options, (r, t) -> handler.handle(r, t, index));
      unis.add(uni);
    }

    return Multi.createFrom()
        .iterable(unis)
        .onItem()
        .transformToUni(u -> u)

        // collect failures so all is executed
        .collectFailures()

        // concatenate so it's returned in order
        .concatenate()

        // collect as list
        .collect()
        .asList();
  }

  // internal implementation of the execute single query
  // deals with consistency, page size & state
  private <OUT> Uni<OUT> executeWithOptions(
      QueryOuterClass.Query query, QueryOptions options, SingleQuerySequence.Handler<OUT> handler) {
    QueryOuterClass.QueryParameters.Builder params =
        QueryOuterClass.QueryParameters.newBuilder(query.getParameters());
    QueryOptions.Type queryType = options.getType();

    // set consistency if not explicitly by query
    if (!params.hasConsistency()) {
      QueryOuterClass.Consistency consistency = getConsistency(queryType);
      QueryOuterClass.ConsistencyValue.Builder consistencyValue =
          QueryOuterClass.ConsistencyValue.newBuilder().setValue(consistency);
      params.setConsistency(consistencyValue);
    }

    // read specific options
    if (QueryOptions.Type.READ.equals(queryType)) {

      // page state
      if (null != options.getPagingState()) {
        params.setPagingState(
            BytesValue.of(ByteString.copyFrom(decodeBase64(options.getPagingState()))));
      }

      // page size
      if (null != options.getPageSize()) {
        int page = Math.min(options.getPageSize(), documentConfig.maxPageSize());
        params.setPageSize(Int32Value.of(page));
      } else {
        params.setPageSize(Int32Value.of(documentConfig.defaultPageSize()));
      }
    }

    QueryOuterClass.Query finalQuery =
        QueryOuterClass.Query.newBuilder(query).setParameters(params).build();
    StargateBridge bridge = stargateRequestInfo.getStargateBridge();
    return bridge
        .executeQuery(finalQuery)

        // map to result set
        .map(QueryOuterClass.Response::getResultSet)

        // if success, handle
        // on failure, pass to handler
        // the handler can re-throw, so execute in try
        .onItemOrFailure()
        .transformToUni(
            (r, t) -> {
              try {
                OUT result = handler.handle(r, t);
                return Uni.createFrom().item(result);
              } catch (Throwable e) {
                return Uni.createFrom().failure(e);
              }
            });
  }

  // gets consistency based on the query type
  private QueryOuterClass.Consistency getConsistency(QueryOptions.Type type) {
    return switch (type) {
      case WRITE -> queriesConfig.consistency().writes();
      case READ -> queriesConfig.consistency().reads();
      case SCHEMA -> queriesConfig.consistency().schemaChanges();
    };
  }

  // decodes base64
  private static byte[] decodeBase64(String base64encoded) {
    return Base64.getDecoder().decode(base64encoded);
  }
}
