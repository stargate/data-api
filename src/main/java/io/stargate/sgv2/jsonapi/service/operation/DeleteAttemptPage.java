package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;

/**
 * A page of results from a delete command, use {@link #builder()} to get a builder to pass to
 * {@link GenericOperation}.
 */
public class DeleteAttemptPage<SchemaT extends TableBasedSchemaObject>
    extends OperationAttemptPage<SchemaT, DeleteAttempt<SchemaT>> {

  private DeleteAttemptPage(
      OperationAttemptContainer<SchemaT, DeleteAttempt<SchemaT>> attempts,
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

    // For tables, there is no way to know how many rows were deleted in CQL,
    // even if we specify the full PK it may delete 0 rows.
    // For now, we will return -1 as the deleted count.When we update collections to use this class
    // we can refactor to return the actual count for them.
    resultBuilder.addStatus(CommandStatus.DELETED_COUNT, -1);
  }

  public static class Builder<SchemaT extends TableBasedSchemaObject>
      extends OperationAttemptPageBuilder<SchemaT, DeleteAttempt<SchemaT>> {

    Builder() {}

    @Override
    public DeleteAttemptPage<SchemaT> getOperationPage() {

      // when we refactor collections to use the OperationAttempt this will need to support
      // returning a document
      // e.g. for findOneAndDelete, for now it is always status only

      return new DeleteAttemptPage<>(
          attempts, CommandResult.statusOnlyBuilder(useErrorObjectV2, debugMode));
    }
  }
}
