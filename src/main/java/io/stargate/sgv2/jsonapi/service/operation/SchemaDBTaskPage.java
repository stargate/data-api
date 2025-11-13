package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskAccumulator;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskPage;
import java.util.function.Supplier;

/**
 * A page of results from a schema modification command, use {@link #builder()} to get a builder to
 * pass to {@link GenericOperation}.
 */
public class SchemaDBTaskPage<TaskT extends SchemaDBTask<SchemaT>, SchemaT extends SchemaObject>
    extends TaskPage<TaskT, SchemaT> {

  private SchemaDBTaskPage(TaskGroup<TaskT, SchemaT> tasks, CommandResultBuilder resultBuilder) {
    super(tasks, resultBuilder);
  }

  /**
   * Gets the {@link TaskAccumulator} for building a {@link SchemaDBTaskPage} for a metadata
   * command.
   *
   * @param taskClass The class of the {@link MetadataDBTask} we are accumulating, this is only
   *     needed to lock the generics in. Param is not actually used.
   * @param commandContext Context used to configure common properties for the {@link
   *     TaskAccumulator}
   * @return A new {@link TaskAccumulator} for building a {@link MetadataDBTaskPage}
   * @param <TaskT> Subtype of {@link MetadataDBTask} to accumulate.
   * @param <SchemaT> Schema object type.
   */
  public static <TaskT extends SchemaDBTask<SchemaT>, SchemaT extends SchemaObject>
      Accumulator<TaskT, SchemaT> accumulator(
          Class<TaskT> taskClass, CommandContext<SchemaT> commandContext) {
    return TaskAccumulator.configureForContext(new Accumulator<>(), commandContext);
  }

  @Override
  protected void buildCommandResult() {
    super.buildCommandResult();

    resultBuilder.addStatus(CommandStatus.OK, taskGroup.allTasksCompleted() ? 1 : 0);
  }

  public static class Accumulator<TaskT extends SchemaDBTask<SchemaT>, SchemaT extends SchemaObject>
      extends TaskAccumulator<TaskT, SchemaT> {

    Accumulator() {}

    @Override
    public Supplier<CommandResult> getResults() {
      return new SchemaDBTaskPage<>(
          tasks, CommandResult.statusOnlyBuilder(useErrorObjectV2, requestTracing));
    }
  }
}
