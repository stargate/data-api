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
public class MetadataAttemptPage<SchemaT extends SchemaObject>
    extends DBTaskPage<MetadataDBTask<SchemaT>, SchemaT> {

  private final boolean showSchema;
  private final CommandStatus statusKey;

  private MetadataAttemptPage(
      TaskGroup<MetadataDBTask<SchemaT>, SchemaT> tasks,
      CommandResultBuilder resultBuilder,
      boolean showSchema,
      CommandStatus statusKey) {
    super(tasks, resultBuilder);

    this.showSchema = showSchema;
    this.statusKey = statusKey;
  }

  public static <SchemaT extends SchemaObject> Accumulator<SchemaT> accumulator(
      CommandContext<SchemaT> commandContext) {
    return TaskAccumulator.configureForContext(
        new MetadataAttemptPage.Accumulator<>(), commandContext);
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

  public static class Accumulator<SchemaT extends SchemaObject>
      extends TaskAccumulator<MetadataDBTask<SchemaT>, SchemaT> {

    private boolean showSchema = false;
    private CommandStatus statusKey;

    Accumulator() {}

    public Accumulator<SchemaT> showSchema(boolean showSchema) {
      this.showSchema = showSchema;
      return this;
    }

    public Accumulator<SchemaT> usingCommandStatus(CommandStatus statusKey) {
      this.statusKey = statusKey;
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public Supplier<CommandResult> getResults() {

      return new MetadataAttemptPage<>(
          tasks,
          CommandResult.statusOnlyBuilder(useErrorObjectV2, debugMode),
          showSchema,
          statusKey);
    }
  }
}
