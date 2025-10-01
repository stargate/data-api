package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.DBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskAccumulator;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;

/**
 * A page of results from a update command, use {@link #builder()} to get a builder to pass to
 * {@link GenericOperation}.
 */
public class UpdateDBTaskPage<SchemaT extends TableSchemaObject>
    extends DBTaskPage<UpdateDBTask<SchemaT>, SchemaT> {

  private UpdateDBTaskPage(
      TaskGroup<UpdateDBTask<SchemaT>, SchemaT> tasks, CommandResultBuilder resultBuilder) {
    super(tasks, resultBuilder);
  }

  public static <SchemaT extends TableSchemaObject> Accumulator<SchemaT> accumulator(
      CommandContext<SchemaT> commandContext) {
    return TaskAccumulator.configureForContext(new Accumulator<>(), commandContext);
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
    if (taskGroup.errorTasks().isEmpty()) {
      resultBuilder.addStatus(CommandStatus.MATCHED_COUNT, 1);
      resultBuilder.addStatus(CommandStatus.MODIFIED_COUNT, 1);
    }
  }

  public static class Accumulator<SchemaT extends TableSchemaObject>
      extends TaskAccumulator<UpdateDBTask<SchemaT>, SchemaT> {

    Accumulator() {}

    @Override
    public UpdateDBTaskPage<SchemaT> getResults() {

      // when we refactor collections to use the OperationAttempt this will need to support
      // returning a document
      // e.g. for findOneAndDelete, for now it is always status only
      return new UpdateDBTaskPage<>(
          tasks, CommandResult.statusOnlyBuilder(useErrorObjectV2, requestTracing));
    }
  }
}
