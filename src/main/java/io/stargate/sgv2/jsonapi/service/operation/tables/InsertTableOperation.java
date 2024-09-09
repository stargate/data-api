package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.InsertOperationPage;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsertTableOperation extends TableMutationOperation {
  private static final Logger LOGGER = LoggerFactory.getLogger(InsertTableOperation.class);

  private final List<TableInsertAttempt> insertAttempts;
  protected final DriverExceptionHandler<TableSchemaObject> driverExceptionHandler;

  // TODO AARON JSON to start with, need a document object
  public InsertTableOperation(
      CommandContext<TableSchemaObject> commandContext,
      DriverExceptionHandler<TableSchemaObject> driverExceptionHandler,
      List<TableInsertAttempt> insertAttempts) {
    super(commandContext);

    this.driverExceptionHandler =
        Objects.requireNonNull(driverExceptionHandler, "driverExceptionHandler cannot be null");
    this.insertAttempts = List.copyOf(insertAttempts);
  }

  /**
   * Uses the provided {@link DriverExceptionHandler} to handle any driver errors that occur during.
   *
   * @param throwable Any throwable, if it is not a {@link
   *     com.datastax.oss.driver.api.core.DriverException} or there is no special handling for it,
   *     it will be returned as is.
   * @return Handler error, turning into a {@link io.stargate.sgv2.jsonapi.exception.APIException}
   *     or the provided <code>throwable
   *     </code>.
   */
  protected RuntimeException maybeHandleDriverError(RuntimeException throwable) {
    return driverExceptionHandler.maybeHandle(commandContext.schemaObject(), throwable);
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {

    var debugMode = commandContext.getConfig(DebugModeConfig.class).enabled();
    var extendedErrors = commandContext.getConfig(OperationsConfig.class).extendError();

    // TODO AARON - this is for unordered, copy from Collection InsertCollectionOperation
    // insertUnordered
    return Multi.createFrom()
        .iterable(insertAttempts)
        // merge to make it parallel
        .onItem()
        .transformToUniAndMerge(
            insertion -> insertRow(dataApiRequestInfo, queryExecutor, insertion))
        // then reduce here
        .collect()
        .in(
            () -> new InsertOperationPage(insertAttempts, false, debugMode, extendedErrors),
            InsertOperationPage::registerCompletedAttempt)
        // use object identity to resolve to Supplier<CommandResult>
        // TODO AARON - not sure what this is doing, original was .map(i -> i)
        .map(Function.identity());
  }

  private Uni<TableInsertAttempt> insertRow(
      DataApiRequestInfo dataApiRequestInfo,
      QueryExecutor queryExecutor,
      TableInsertAttempt insertAttempt) {

    // First things first: did we already fail? If so, propagate
    if (insertAttempt.failure().isPresent()) {
      return Uni.createFrom().failure(insertAttempt.failure().get());
    }

    // If we did not fail, then we should have a row, test that
    if (insertAttempt.row().isEmpty()) {
      return Uni.createFrom()
          .failure(new IllegalStateException("InsertAttempt has no row, and no failure"));
    }

    // bind and execute
    var boundStatement = buildInsertStatement(queryExecutor, insertAttempt);

    LOGGER.warn("INSERT CQL: {}", boundStatement.getQuery());
    LOGGER.warn("INSERT VALUES: {}", boundStatement.getPositionalValues());

    // TODO: AARON What happens to errors here?
    return queryExecutor
        .executeWrite(dataApiRequestInfo, boundStatement)
        .onItemOrFailure()
        .transform(
            (result, t) -> {
              return switch (t) {
                case null -> insertAttempt;
                case RuntimeException runtimeException ->
                    (TableInsertAttempt)
                        insertAttempt.maybeAddFailure(maybeHandleDriverError(runtimeException));
                default -> (TableInsertAttempt) insertAttempt.maybeAddFailure(t);
              };
            })
        .onItemOrFailure()
        .transform((ia, throwable) -> (TableInsertAttempt) ia.maybeAddFailure(throwable));
  }

  private SimpleStatement buildInsertStatement(
      QueryExecutor queryExecutor, TableInsertAttempt insertAttempt) {

    InsertInto insertInto =
        QueryBuilder.insertInto(
            commandContext.schemaObject().tableMetadata.getKeyspace(),
            commandContext.schemaObject().tableMetadata.getName());

    List<Object> positionalValues = new ArrayList<>();
    RegularInsert regularInsert =
        insertAttempt.getInsertValuesCQLClause().apply(insertInto, positionalValues);
    return SimpleStatement.newInstance(regularInsert.asCql(), positionalValues.toArray());
  }
}
