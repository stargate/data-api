package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.DBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskAccumulator;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;

/**
 * A page of results from a delete command, use {@link #builder()} to get a builder to pass to
 * {@link GenericOperation}.
 */
public class DeleteDBTaskPage<SchemaT extends TableBasedSchemaObject>
    extends DBTaskPage<DeleteDBTask<SchemaT>, SchemaT> {

  private DeleteDBTaskPage(
      TaskGroup<DeleteDBTask<SchemaT>, SchemaT> tasks, CommandResultBuilder resultBuilder) {
    super(tasks, resultBuilder);
  }

  public static <SchemaT extends TableBasedSchemaObject> Accumulator<SchemaT> accumulator(
      CommandContext<SchemaT> commandContext) {
    return TaskAccumulator.configureForContext(new Accumulator<>(), commandContext);
  }

  @Override
  protected void buildCommandResult() {

    // set errors and warnings
    super.buildCommandResult();

    // For tables, there is no way to know how many rows were deleted in CQL,
    // even if we specify the full PK it may delete 0 rows.
    // For now, we will return -1 as the deleted count.When we update collections to use this class
    // we can refactor to return the actual count for them.
    // If there is error, we won't add this status.
    if (tasks.errorTasks().isEmpty()) {
      resultBuilder.addStatus(CommandStatus.DELETED_COUNT, -1);
    }
  }

  public static class Accumulator<SchemaT extends TableBasedSchemaObject>
      extends TaskAccumulator<DeleteDBTask<SchemaT>, SchemaT> {

    Accumulator() {}

    @Override
    public DeleteDBTaskPage<SchemaT> getResults() {

      // when we refactor collections to use the OperationAttempt this will need to support
      // returning a document
      // e.g. for findOneAndDelete, for now it is always status only

      return new DeleteDBTaskPage<>(
          tasks, CommandResult.statusOnlyBuilder(useErrorObjectV2, debugMode, requestTracing));
    }
  }
}
