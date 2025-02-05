package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.DBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskAccumulator;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;

/**
 * A page of results from a schema modification command, use {@link #builder()} to get a builder to
 * pass to {@link GenericOperation}.
 */
public class SchemaDBTaskPage<SchemaT extends SchemaObject>
    extends DBTaskPage<SchemaDBTask<SchemaT>, SchemaT> {

  private SchemaDBTaskPage(
      TaskGroup<SchemaDBTask<SchemaT>, SchemaT> tasks, CommandResultBuilder resultBuilder) {
    super(tasks, resultBuilder);
  }

  public static <SchemaT extends SchemaObject> Accumulator<SchemaT> accumulator(
      CommandContext<SchemaT> commandContext) {
    return TaskAccumulator.configureForContext(new Accumulator<>(), commandContext);
  }

  @Override
  protected void buildCommandResult() {
    super.buildCommandResult();

    resultBuilder.addStatus(CommandStatus.OK, tasks.allTasksCompleted() ? 1 : 0);
  }

  public static class Accumulator<SchemaT extends SchemaObject>
      extends TaskAccumulator<SchemaDBTask<SchemaT>, SchemaT> {

    Accumulator() {}

    @Override
    public SchemaDBTaskPage<SchemaT> getResults() {
      return new SchemaDBTaskPage<>(
          tasks, CommandResult.statusOnlyBuilder(useErrorObjectV2, debugMode));
    }
  }
}
