package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.Optional;
import java.util.function.Supplier;

public class SchemaAttemptPage<SchemaT extends SchemaObject> implements Supplier<CommandResult> {

  private final OperationAttemptContainer<SchemaT, SchemaAttempt<SchemaT>> attempts;
  private final boolean returnSuccess;
  private final CommandResultBuilder resultBuilder;

  private SchemaAttemptPage(
      OperationAttemptContainer<SchemaT, SchemaAttempt<SchemaT>> attempts,
      CommandResultBuilder resultBuilder,
      boolean returnSuccess) {

    this.attempts = attempts;
    this.returnSuccess = returnSuccess;
    this.resultBuilder = resultBuilder;
  }

  public static <SchemaT extends SchemaObject> Builder<SchemaT> builder() {
    return new Builder<>();
  }

  @Override
  public CommandResult get() {

    resultBuilder.addStatus(CommandStatus.OK, returnSuccess ? 1 : 0);

    attempts.errorAttempts().stream()
        .map(OperationAttempt::failure)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .forEach(resultBuilder::addThrowable);

    return resultBuilder.build();
  }

  public static class Builder<SchemaT extends SchemaObject>
      extends OperationAttemptPageBuilder<SchemaT, SchemaAttempt<SchemaT>> {

    Builder() {}

    @Override
    public SchemaAttemptPage<SchemaT> getOperationPage() {

      attempts.checkAllAttemptsTerminal();

      var resultBuilder =
          new CommandResultBuilder(
              CommandResultBuilder.ResponseType.STATUS_ONLY, useErrorObjectV2, debugMode);

      return new SchemaAttemptPage<>(attempts, resultBuilder, attempts.allAttemptsCompleted());
    }
  }
}
