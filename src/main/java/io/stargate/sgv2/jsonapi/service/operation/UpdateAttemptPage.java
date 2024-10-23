package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;

/**
 * A page of results from a update command, use {@link #builder()} to get a builder to pass to
 * {@link GenericOperation}.
 */
public class UpdateAttemptPage<SchemaT extends TableBasedSchemaObject>
    extends OperationAttemptPage<SchemaT, UpdateAttempt<SchemaT>> {

  private UpdateAttemptPage(
      OperationAttemptContainer<SchemaT, UpdateAttempt<SchemaT>> attempts,
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

    // Because CQL UPDATE is a upsert it will always match and always modify a row, even
    // if that means inserting
    // However - we do not know if an upsert happened :(
    // NOTE when update collection uses operation attempt this will get more complex
    // If there is error, we won't add this status.
    if (attempts.errorAttempts().isEmpty()) {
      resultBuilder.addStatus(CommandStatus.MATCHED_COUNT, 1);
      resultBuilder.addStatus(CommandStatus.MODIFIED_COUNT, 1);
    }
  }

  public static class Builder<SchemaT extends TableBasedSchemaObject>
      extends OperationAttemptPageBuilder<SchemaT, UpdateAttempt<SchemaT>> {

    Builder() {}

    @Override
    public UpdateAttemptPage<SchemaT> getOperationPage() {

      // when we refactor collections to use the OperationAttempt this will need to support
      // returning a document
      // e.g. for findOneAndDelete, for now it is always status only

      return new UpdateAttemptPage<>(
          attempts, CommandResult.statusOnlyBuilder(useErrorObjectV2, debugMode));
    }
  }
}
