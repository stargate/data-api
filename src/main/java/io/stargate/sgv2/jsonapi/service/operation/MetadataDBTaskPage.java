package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.DBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskAccumulator;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import java.util.function.Supplier;

/**
 * A page of results for metadata based command, use {@link #builder()} to get a builder to pass to
 * {@link GenericOperation}.
 */
public class MetadataDBTaskPage<TaskT extends MetadataDBTask<SchemaT>, SchemaT extends SchemaObject>
    extends DBTaskPage<TaskT, SchemaT> {

  private final boolean showSchema;
  private final CommandStatus statusKey;

  private MetadataDBTaskPage(
      TaskGroup<TaskT, SchemaT> tasks,
      CommandResultBuilder resultBuilder,
      boolean showSchema,
      CommandStatus statusKey) {
    super(tasks, resultBuilder);

    this.showSchema = showSchema;
    this.statusKey = statusKey;
  }

  /**
   * Gets the {@link TaskAccumulator} for building a {@link MetadataDBTaskPage} for a metadata
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
  public static <TaskT extends MetadataDBTask<SchemaT>, SchemaT extends SchemaObject>
      Accumulator<TaskT, SchemaT> accumulator(
          Class<TaskT> taskClass, CommandContext<SchemaT> commandContext) {
    return TaskAccumulator.configureForContext(
        new MetadataDBTaskPage.Accumulator<>(), commandContext);
  }

  @Override
  protected void buildCommandResult() {
    addTaskWarningsToResult();
    addTaskErrorsToResult();

    var metadataAttempts = tasks.completedTasks();
    if (metadataAttempts.size() > 1) {
      throw new IllegalArgumentException("Only one attempt is expected for metadata commands");
    }
    if (!metadataAttempts.isEmpty()) {
      if (showSchema) {
        resultBuilder.addStatus(statusKey, tasks.getFirst().getSchema());
      } else {
        resultBuilder.addStatus(statusKey, tasks.getFirst().getNames());
      }
    }
  }

  public static class Accumulator<
          TaskT extends MetadataDBTask<SchemaT>, SchemaT extends SchemaObject>
      extends TaskAccumulator<TaskT, SchemaT> {

    private boolean showSchema = false;
    private CommandStatus statusKey;

    Accumulator() {}

    public Accumulator<TaskT, SchemaT> showSchema(boolean showSchema) {
      this.showSchema = showSchema;
      return this;
    }

    public Accumulator<TaskT, SchemaT> usingCommandStatus(CommandStatus statusKey) {
      this.statusKey = statusKey;
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public Supplier<CommandResult> getResults() {

      return new MetadataDBTaskPage<>(
          tasks,
          CommandResult.statusOnlyBuilder(requestTracing),
          showSchema,
          statusKey);
    }
  }
}
