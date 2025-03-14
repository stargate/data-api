package io.stargate.sgv2.jsonapi.api.model.command.tracing;

import static io.stargate.sgv2.jsonapi.util.CqlPrintUtil.trimmedCql;
import static io.stargate.sgv2.jsonapi.util.CqlPrintUtil.trimmedPositionalValues;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.QueryTrace;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.service.cqldriver.AccumulatingAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.operation.tasks.Task;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/** Reusable factories for creating {@link RequestTracing.TraceMessage} instances. */
public abstract class DBTraceMessages {

  private static void maybeTrace(
      RequestTracing requestTracing,
      Supplier<Recordable> recordableSupplier,
      String messageTemplate,
      Object... args) {
    requestTracing.maybeTrace(
        () ->
            new RequestTracing.TraceMessage(
                messageTemplate.formatted(args), recordableSupplier.get()));
  }

  public static void executingStatement(
      RequestTracing requestTracing, SimpleStatement statement, Task<?> task) {
    executingStatement(
        requestTracing, statement, "Executing statement for task %s", task.taskDesc());
  }

  public static void executingStatement(
      RequestTracing requestTracing,
      SimpleStatement statement,
      String messageTemplate,
      Object... args) {
    var recordable =
        Recordable.mapSupplier(
            () ->
                Map.of(
                    "cql",
                    statement == null ? "null" : trimmedCql(statement),
                    "params",
                    statement == null ? "null" : trimmedPositionalValues(statement)));
    maybeTrace(requestTracing, recordable, messageTemplate, args);
  }

  /**
   * Returns a {@link Uni} that can be passed to {@link Uni#call(Function)} that will retrieve the
   * CQL trace if it was enabled for the request and trace it using the {@link RequestTracing}
   *
   * <p>Examaple usage, where the supplier is {@code Supplier<Uni<AsyncResultSet>>}
   *
   * <pre>
   *    public Uni<AsyncResultSet> get() {
   *
   *       DBTraceMessages.executingStatement(commandContext.requestTracing(), statement, task);
   *
   *       return supplier
   *           .get()
   *           .onItem()
   *           .call(
   *               asyncResultSet ->
   *                   DBTraceMessages.maybeCqlTrace(
   *                       commandContext.requestTracing(), asyncResultSet, task));
   *     }
   *   }
   * </pre>
   *
   * @param requestTracing
   * @param asyncResultSet
   * @return
   */
  public static Uni<?> maybeCqlTrace(
      RequestTracing requestTracing,
      AsyncResultSet asyncResultSet,
      String messageTemplate,
      Object... args) {

    // aaron 10 march 2025 - this is still experimental and I think sometimes it is
    // called after the result set has completed and the executionInfo is null
    // The AccumulatingAsyncResultSet will not have the execution info, and will throw
    // UnsupportedOperationException - because the interface says the return from
    // getExecutionInfo cannot be null
    try {
      if (asyncResultSet.getExecutionInfo() == null
          || asyncResultSet.getExecutionInfo().getTracingId() == null) {
        return Uni.createFrom().voidItem();
      }
    } catch (UnsupportedOperationException e) {
      if (asyncResultSet instanceof AccumulatingAsyncResultSet) {
        return Uni.createFrom().voidItem();
      }
      throw e;
    }

    return Uni.createFrom()
        .completionStage(() -> asyncResultSet.getExecutionInfo().getQueryTraceAsync())
        .onItemOrFailure()
        .invoke((trace, throwable) -> {
          if (throwable == null) {
            cqltrace(requestTracing, trace, messageTemplate, args);
          } else {
            // getting the trace will try up to 5 times by default, and then throw a IllegalStateException
            maybeTrace(requestTracing, Recordable.mapSupplier( () -> Map.of("error", throwable)), "Error retrieving CQL trace");
          }
        });
  }

  public static Uni<?> maybeCqlTrace(
      RequestTracing requestTracing, AsyncResultSet asyncResultSet, Task<?> task) {
    return maybeCqlTrace(
        requestTracing, asyncResultSet, "Statement trace for task %s", task.taskDesc());
  }

  public static void cqltrace(
      RequestTracing requestTracing,
      QueryTrace queryTrace,
      String messageTemplate,
      Object... args) {
    requestTracing.maybeTrace(
        (objectMapper) ->
            new RequestTracing.TraceMessage(
                messageTemplate.formatted(args),
                objectMapper.convertValue(queryTrace, JsonNode.class)));
  }
}
