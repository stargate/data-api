package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
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

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {

    var debugMode = commandContext.getConfig(DebugModeConfig.class).enabled();
    var extendedErrors = commandContext.getConfig(OperationsConfig.class).extendError();

    // TODO AARON - for now we create the CommandQueryExecutor here , later change the Operation
    // interface
    CommandQueryExecutor commandQueryExecutor =
        new CommandQueryExecutor(
            queryExecutor.getCqlSessionCache(),
            new RequestContext(
                dataApiRequestInfo.getTenantId(), dataApiRequestInfo.getCassandraToken()),
            CommandQueryExecutor.QueryTarget.TABLE);

    // TODO AARON - IS THIS IN parallel ?
    return Multi.createFrom()
        .iterable(insertAttempts)
        // merge to make it parallel
        .onItem()
        .transformToUniAndMerge(
            insertAttempt -> insertAttempt.execute(commandQueryExecutor, driverExceptionHandler))
        .onItem()
        .transform(OperationAttempt::setSkippedIfReady)
        .onItem()
        .transform(OperationAttempt::checkTerminal)
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
