package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;

/**
 * A page of results from a deleteMany(empty filter -> truncate) command, use {@link #builder()} to
 * get a builder to pass to {@link GenericOperation}.
 */
public class TruncateAttemptPage<SchemaT extends TableBasedSchemaObject>
    extends OperationAttemptPage<SchemaT, TruncateAttempt<SchemaT>> {

  private TruncateAttemptPage(
      OperationAttemptContainer<SchemaT, TruncateAttempt<SchemaT>> attempts,
      CommandResultBuilder resultBuilder) {
    super(attempts, resultBuilder);
  }

  public static <SchemaT extends TableBasedSchemaObject> Builder<SchemaT> builder() {
    return new Builder<>();
  }

  @Override
  protected void buildCommandResult() {

    // set errors and warnings
    super.buildCommandResult();

    // truncate a table, set delete_count status as -1
    if (attempts.errorAttempts().isEmpty()) {
      resultBuilder.addStatus(CommandStatus.DELETED_COUNT, -1);
    }
  }

  public static class Builder<SchemaT extends TableBasedSchemaObject>
      extends OperationAttemptPageBuilder<SchemaT, TruncateAttempt<SchemaT>> {

    Builder() {}

    @Override
    public TruncateAttemptPage<SchemaT> getOperationPage() {

      return new TruncateAttemptPage<>(
          attempts, CommandResult.statusOnlyBuilder(useErrorObjectV2, debugMode));
    }
  }
}
