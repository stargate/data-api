package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

public class SchemaAttemptPage<SchemaT extends SchemaObject>
    extends OperationAttemptPage<SchemaT, SchemaAttempt<SchemaT>> {

  private final boolean returnSuccess;

  private SchemaAttemptPage(
      OperationAttemptContainer<SchemaT, SchemaAttempt<SchemaT>> attempts,
      CommandResultBuilder resultBuilder,
      boolean returnSuccess) {
    super(attempts, resultBuilder);
    this.returnSuccess = returnSuccess;
  }

  public static <SchemaT extends SchemaObject> Builder<SchemaT> builder() {
    return new Builder<>();
  }

  @Override
  protected void buildCommandResult() {
    super.buildCommandResult();

    resultBuilder.addStatus(CommandStatus.OK, returnSuccess ? 1 : 0);
  }

  public static class Builder<SchemaT extends SchemaObject>
      extends OperationAttemptPageBuilder<SchemaT, SchemaAttempt<SchemaT>> {

    Builder() {}

    @Override
    public SchemaAttemptPage<SchemaT> getOperationPage() {

      attempts.throwIfNotAllTerminal();

      var resultBuilder =
          new CommandResultBuilder(
              CommandResultBuilder.ResponseType.STATUS_ONLY, useErrorObjectV2, debugMode);

      return new SchemaAttemptPage<>(attempts, resultBuilder, attempts.allAttemptsCompleted());
    }
  }
}
