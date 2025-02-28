package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.function.Supplier;

/**
 * The page of results from running a group of {@link CompositeTask} tasks, called outer because
 * each of the CompositeTasks has an inner page for the tasks they run.
 *
 * <p>This is the page responsible for getting final result of the command out from one of the inner
 * operations.
 *
 * @param <SchemaT>
 */
public class CompositeTaskOuterPage<SchemaT extends SchemaObject>
    extends TaskPage<CompositeTask<?, SchemaT>, SchemaT> {

  private CompositeTaskOuterPage(
      TaskGroup<CompositeTask<?, SchemaT>, SchemaT> tasks, CommandResultBuilder resultBuilder) {
    super(tasks, resultBuilder);
  }

  public static <SchemaT extends SchemaObject> Accumulator<SchemaT> accumulator(
      CommandContext<SchemaT> commandContext) {

    return TaskAccumulator.configureForContext(new Accumulator<>(), commandContext);
  }

  @Override
  public CommandResult get() {

    if (!tasks.errorTasks().isEmpty()) {
      // we have some failed tasks, there are failed CompositeTasks's that have lifted errors
      // from their inner tasks
      // the superclass will build a basic response with errors and warnings, that is what we need
      return super.get();
    }

    // the last composite task is the one that will build the results of running all the composite
    // tasks.
    return tasks.getLast().lastTaskAccumulator().getResults().get();
  }

  public static class Accumulator<SchemaT extends SchemaObject>
      extends TaskAccumulator<CompositeTask<?, SchemaT>, SchemaT> {

    protected Accumulator() {}

    @Override
    public Supplier<CommandResult> getResults() {

      // See the outer Page class, if there is a failure, then we add the errors and warnings the
      // Composite
      // tasks have lifted from their internal tasks. This is a status only result.

      // If not failure, we get the result from the lastAccumulator so this command builder is
      // ignored
      return new CompositeTaskOuterPage<>(
          tasks, CommandResult.statusOnlyBuilder(useErrorObjectV2, debugMode, requestTracing));
    }
  }
}
