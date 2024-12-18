package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

/**
 * A page of results from a schema modification command, use {@link #builder()} to get a builder to
 * pass to {@link GenericOperation}.
 */
public class SchemaAttemptPage<SchemaT extends SchemaObject>
    extends OperationAttemptPage<SchemaT, SchemaAttempt<SchemaT>> {

  private SchemaAttemptPage(
      OperationAttemptContainer<SchemaT, SchemaAttempt<SchemaT>> attempts,
      CommandResultBuilder resultBuilder) {
    super(attempts, resultBuilder);
  }

  public static <SchemaT extends SchemaObject> Builder<SchemaT> builder() {
    return new Builder<>();
  }

  @Override
  protected void buildCommandResult() {
    super.buildCommandResult();

    resultBuilder.addStatus(CommandStatus.OK, attempts.allAttemptsCompleted() ? 1 : 0);
  }

  public static class Builder<SchemaT extends SchemaObject>
      extends OperationAttemptPageBuilder<SchemaT, SchemaAttempt<SchemaT>> {

    Builder() {}

    @Override
    public SchemaAttemptPage<SchemaT> getOperationPage() {
      return new SchemaAttemptPage<>(
          attempts, CommandResult.statusOnlyBuilder(useErrorObjectV2, debugMode));
    }
  }
}
