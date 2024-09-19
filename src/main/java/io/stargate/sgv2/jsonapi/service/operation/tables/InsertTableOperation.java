package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.InsertAttempt;
import io.stargate.sgv2.jsonapi.service.operation.InsertOperationPage;
import io.stargate.sgv2.jsonapi.service.operation.OperationAttempt;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsertTableOperation<SchemaT extends TableBasedSchemaObject>
    extends TableMutationOperation {
  private static final Logger LOGGER = LoggerFactory.getLogger(InsertTableOperation.class);

  private final List<InsertAttempt<SchemaT>> insertAttempts;
  protected final DriverExceptionHandler<SchemaT> driverExceptionHandler;
  private final boolean returnDocumentResponses;

  public InsertTableOperation(
      CommandContext<TableSchemaObject> commandContext,
      DriverExceptionHandler<SchemaT> driverExceptionHandler,
      List<? extends InsertAttempt<SchemaT>> insertAttempts,
      boolean returnDocumentResponses) {
    super(commandContext);

    this.driverExceptionHandler =
        Objects.requireNonNull(driverExceptionHandler, "driverExceptionHandler cannot be null");
    this.insertAttempts = List.copyOf(insertAttempts);
    this.returnDocumentResponses = returnDocumentResponses;
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
  //  protected RuntimeException maybeHandleDriverError(RuntimeException throwable) {
  //    return driverExceptionHandler.maybeHandle(commandContext.schemaObject(), throwable);
  //  }

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
            insertAttempt ->
                insertAttempt.execute(dataApiRequestInfo, queryExecutor, driverExceptionHandler))
        .onItem()
        .transform(OperationAttempt::verifyComplete)
        // then reduce here
        .collect()
        .in(
            () ->
                new InsertOperationPage<>(
                    insertAttempts, returnDocumentResponses, debugMode, extendedErrors),
            InsertOperationPage::registerCompletedAttempt)
        // use object identity to resolve to Supplier<CommandResult>
        // TODO AARON - not sure what this is doing, original was .map(i -> i)
        .map(Function.identity());
  }
}
