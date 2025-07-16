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
import java.util.function.Supplier;

/**
 * A page of results from a deleteMany(empty filter -> truncate) command, use {@link #builder()} to
 * get a builder to pass to {@link GenericOperation}.
 */
public class TruncateDBTaskPage<
        TaskT extends TruncateDBTask<SchemaT>, SchemaT extends TableBasedSchemaObject>
    extends DBTaskPage<TaskT, SchemaT> {

  private TruncateDBTaskPage(TaskGroup<TaskT, SchemaT> tasks, CommandResultBuilder resultBuilder) {
    super(tasks, resultBuilder);
  }

  /**
   * Gets the {@link TaskAccumulator} for building a {@link TruncateDBTaskPage} for a metadata
   * command.
   *
   * @param taskClass The class of the {@link TruncateDBTask} we are accumulating, this is only
   *     needed to lock the generics in. Param is not actually used.
   * @param commandContext Context used to configure common properties for the {@link
   *     TaskAccumulator}
   * @return A new {@link TaskAccumulator} for building a {@link TruncateDBTaskPage}
   * @param <TaskT> Subtype of {@link TruncateDBTask} to accumulate.
   * @param <SchemaT> Schema object type.
   */
  public static <TaskT extends TruncateDBTask<SchemaT>, SchemaT extends TableSchemaObject>
      Accumulator<TaskT, SchemaT> accumulator(
          Class<TaskT> taskClass, CommandContext<SchemaT> commandContext) {
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

  public static class Accumulator<
          TaskT extends TruncateDBTask<SchemaT>, SchemaT extends TableBasedSchemaObject>
      extends TaskAccumulator<TaskT, SchemaT> {

    Accumulator() {}

    @Override
    public Supplier<CommandResult> getResults() {

      return new TruncateDBTaskPage<>(
          tasks, CommandResult.statusOnlyBuilder(useErrorObjectV2, requestTracing));
    }
  }
}
