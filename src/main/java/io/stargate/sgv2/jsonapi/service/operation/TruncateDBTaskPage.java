package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.DBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskAccumulator;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;

/**
 * A page of results from a deleteMany(empty filter -> truncate) command, use {@link #builder()} to
 * get a builder to pass to {@link GenericOperation}.
 */
public class TruncateDBTaskPage<SchemaT extends TableBasedSchemaObject>
    extends DBTaskPage<TruncateDBTask<SchemaT>, SchemaT> {

  private TruncateDBTaskPage(
      TaskGroup<TruncateDBTask<SchemaT>, SchemaT> tasks,
      CommandResultBuilder resultBuilder) {
    super(tasks, resultBuilder);
  }


  public static <SchemaT extends TableSchemaObject> Accumulator<SchemaT> accumulator(CommandContext<SchemaT> commandContext) {
    return TaskAccumulator.configureForContext(new Accumulator<>(), commandContext);
  }

  @Override
  protected void buildCommandResult() {

    // set errors and warnings
    super.buildCommandResult();

    // truncate a table, set delete_count status as -1
    if (tasks.errorTasks().isEmpty()) {
      resultBuilder.addStatus(CommandStatus.DELETED_COUNT, -1);
    }
  }

  public static class Accumulator<SchemaT extends TableBasedSchemaObject>
      extends TaskAccumulator<TruncateDBTask<SchemaT>, SchemaT> {

    Accumulator() {}

    @Override
    public TruncateDBTaskPage<SchemaT> getResults() {

      return new TruncateDBTaskPage<>(
          tasks, CommandResult.statusOnlyBuilder(useErrorObjectV2, debugMode));
    }
  }
}
