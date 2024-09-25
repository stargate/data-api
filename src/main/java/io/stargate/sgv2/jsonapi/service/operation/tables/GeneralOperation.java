package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.operation.*;
import java.util.Objects;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeneralOperation<
        SchemaT extends SchemaObject, AttemptT extends OperationAttempt<AttemptT, SchemaT>>
    implements Operation {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeneralOperation.class);

  private final CommandContext<SchemaT> commandContext;
  private final DriverExceptionHandler<SchemaT> driverExceptionHandler;
  private final OperationAttemptContainer<SchemaT, AttemptT> attempts;

  private final OperationAttemptPageBuilder<SchemaT, AttemptT> pageBuilder;

  public GeneralOperation(
      CommandContext<SchemaT> commandContext,
      DriverExceptionHandler<SchemaT> driverExceptionHandler,
      OperationAttemptContainer<SchemaT, AttemptT> attempts,
      OperationAttemptPageBuilder<SchemaT, AttemptT> pageBuilder) {

    this.commandContext = commandContext;
    this.driverExceptionHandler =
        Objects.requireNonNull(driverExceptionHandler, "driverExceptionHandler cannot be null");
    this.attempts = attempts;
    this.pageBuilder = Objects.requireNonNull(pageBuilder, "pageBuilder cannot be null");
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {

    return startMulti(dataApiRequestInfo, queryExecutor)
        .onItem()
        .transform(OperationAttempt::setSkippedIfReady)
        .collect()
        .in(() -> pageBuilder, OperationAttemptAccumulator::accumulate)
        .onItem()
        .transform(OperationAttemptPageBuilder::getOperationPage);
  }

  protected Multi<AttemptT> startMulti(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {

    // TODO - for now we create the CommandQueryExecutor here , later change the Operation interface
    var commandQueryExecutor =
        new CommandQueryExecutor(
            queryExecutor.getCqlSessionCache(),
            new RequestContext(
                dataApiRequestInfo.getTenantId(), dataApiRequestInfo.getCassandraToken()),
            CommandQueryExecutor.QueryTarget.TABLE);

    var attemptMulti = Multi.createFrom().iterable(attempts).onItem();

    LOGGER.debug("startMulti() starting Multi for attempts={}", attempts.toString());

    if (attempts.getSequentialProcessing()) {
      return attemptMulti.transformToUniAndConcatenate(
          attempt -> {
            var failFast = attempts.shouldFailFast();
            LOGGER.debug(
                "startMulti() - dequeing attempt failFast={}, attempt={}",
                failFast,
                attempt.toString());
            if (failFast) {
              return Uni.createFrom().item(attempt.setSkippedIfReady());
            }
            return attempt.execute(commandQueryExecutor, driverExceptionHandler);
          });
    }
    return attemptMulti.transformToUniAndMerge(
        readAttempt -> readAttempt.execute(commandQueryExecutor, driverExceptionHandler));
  }
}
