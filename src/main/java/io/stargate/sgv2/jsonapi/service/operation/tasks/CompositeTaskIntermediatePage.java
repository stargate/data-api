package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The results of running an intermediate {@link CompositeTask}, this is one where the results of
 * the inner task are <b>note</b> the results of the final pipline for the operation.
 *
 * <p>e.g. when we are doing the vectorizing work for before an insert.
 *
 * <p>In these cases we want to cary the tasks of the inner operation, so we can look at the results
 * of the inner operation without building a {@link
 * io.stargate.sgv2.jsonapi.api.model.command.CommandResult}
 */
public class CompositeTaskIntermediatePage<
        InnerTaskT extends Task<SchemaT>, SchemaT extends SchemaObject>
    implements Supplier<CommandResult> {

  private TaskGroup<InnerTaskT, SchemaT> tasks;
  private final TaskAccumulator<InnerTaskT, SchemaT> lastTaskAccumulator;
  private CommandResult lastTaskResult;

  private CompositeTaskIntermediatePage(
      TaskGroup<InnerTaskT, SchemaT> tasks,
      TaskAccumulator<InnerTaskT, SchemaT> lastTaskAccumulator) {
    this.tasks = tasks;
    this.lastTaskAccumulator = lastTaskAccumulator;
  }

  /**
   * Gets the {@link TaskAccumulator} for building a {@link CompositeTaskIntermediatePage}
   *
   * @param taskClass The class of the task for the inner operation we are accumulating, this is
   *     only needed to lock the generics in. Param is not actually used.
   * @param commandContext Context used to configure common properties for the {@link
   *     TaskAccumulator}
   * @return A new {@link TaskAccumulator} for building a {@link CompositeTaskIntermediatePage}
   * @param <TaskT> Subtype of inner operation {@link Task} to accumulate.
   * @param <SchemaT> Schema object type.
   */
  public static <InnerTaskT extends Task<SchemaT>, SchemaT extends SchemaObject>
      Accumulator<InnerTaskT, SchemaT> accumulator(CommandContext<SchemaT> commandContext) {

    return TaskAccumulator.configureForContext(new Accumulator<>(), commandContext);
  }

  @Override
  public CommandResult get() {

    return lastTaskAccumulator.getResults().get();
  }

  void fetchLastTaskResults() {
    if (lastTaskAccumulator == null) {
      throw new IllegalStateException("lastTaskAccumulator is null, this should not happen");
    }

    lastTaskResult = lastTaskAccumulator.getResults().get();
  }

  Optional<InnerTaskT> firstFailedTask() {
    return tasks.errorTasks().stream().findFirst();
  }

  public static class Accumulator<InnerTaskT extends Task<SchemaT>, SchemaT extends SchemaObject>
      extends TaskAccumulator<InnerTaskT, SchemaT> {

    private TaskAccumulator<InnerTaskT, SchemaT> lastTaskAccumulator;

    protected Accumulator() {}

    public Accumulator<InnerTaskT, SchemaT> withLastTaskAccumulator(
        TaskAccumulator<InnerTaskT, SchemaT> lastTaskAccumulator) {
      this.lastTaskAccumulator = lastTaskAccumulator;
      return this;
    }

    @Override
    public void accumulate(InnerTaskT task) {
      if (lastTaskAccumulator != null) {
        lastTaskAccumulator.accumulate(task);
      }
      super.accumulate(task);
    }

    @Override
    public Supplier<CommandResult> getResults() {
      throw new IllegalStateException(
          "Supplier<CommandResult> getResults() should not be called on CompositeTaskIntermediatePage.Accumulator");
    }

    public Supplier<CompositeTaskIntermediatePage<InnerTaskT, SchemaT>> intermediatePage() {
      // We are here to accumulate the inner tasks

      return () -> new CompositeTaskIntermediatePage<>(tasks, lastTaskAccumulator);
    }
  }
}
