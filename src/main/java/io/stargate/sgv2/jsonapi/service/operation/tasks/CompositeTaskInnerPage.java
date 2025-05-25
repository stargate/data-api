package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObject;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The results of running the inner tasks for a {@link CompositeTask}, and a way to smuggle the
 * result of the running the last composite task that has the results for the whole command.
 *
 * <p>Create via the {@link #accumulator(CommandContext)} , check it's docs for how it is to be
 * used.
 *
 * <p>This page does not know how to build a {@link CommandResult}, it's job is to:
 *
 * <ul>
 *   <li>Gather the inner tasks that have completed, so we can see if any failed to lift their
 *       errors.
 *   <li>Hold the last task accumulator, so that if the composite task is the last we can use that
 *       to get the results for the user. Called from {@link CompositeTaskOuterPage}
 * </ul>
 */
public class CompositeTaskInnerPage<InnerTaskT extends Task<SchemaT>, SchemaT extends SchemaObject>
    implements Supplier<CommandResult> {

  private TaskGroup<InnerTaskT, SchemaT> tasks;
  private final TaskAccumulator<InnerTaskT, SchemaT> lastTaskAccumulator;

  private CompositeTaskInnerPage(
      TaskGroup<InnerTaskT, SchemaT> tasks,
      TaskAccumulator<InnerTaskT, SchemaT> lastTaskAccumulator) {
    this.tasks = tasks;
    this.lastTaskAccumulator = lastTaskAccumulator;
  }

  public static <InnerTaskT extends Task<SchemaT>, SchemaT extends SchemaObject>
      Accumulator<InnerTaskT, SchemaT> accumulator(CommandContext<SchemaT> commandContext) {

    return TaskAccumulator.configureForContext(new Accumulator<>(), commandContext);
  }

  @Override
  public CommandResult get() {
    Objects.requireNonNull(
        lastTaskAccumulator,
        "CompositeTaskInnerPage.get() called when the lastTaskAccumulator is null, this is not last task?");
    return lastTaskAccumulator.getResults().get();
  }

  Optional<InnerTaskT> firstFailedTask() {
    return tasks.errorTasks().stream().findFirst();
  }

  /**
   * Special accumulator for use wit the {@link CompositeTask}, it wil accumulate into both its own
   * list of completed tasks and if configured the accumulator for the last task so the final
   * results can be built.
   *
   * @param <InnerTaskT>
   * @param <SchemaT>
   */
  public static class Accumulator<InnerTaskT extends Task<SchemaT>, SchemaT extends SchemaObject>
      extends TaskAccumulator<InnerTaskT, SchemaT> {

    private TaskAccumulator<InnerTaskT, SchemaT> lastTaskAccumulator;

    protected Accumulator() {}

    /**
     * Set this if the CompositeTask is the last task in the operation, all accumulated tasks will
     * be passed through to the lastTaskAccumulator, and it will also be given to the {@link
     * CompositeTaskInnerPage} so it can get the results.
     *
     * @param lastTaskAccumulator The accumulator for the last task in the operation, if null is
     *     ignored
     * @return This, for chaining.
     */
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
          "Supplier<CommandResult> getResults() should not be called on CompositeTaskInnerPage.Accumulator");
    }

    /** See the {@link CompositeTask.CompositeTaskResultSupplier} for usage. */
    Supplier<CompositeTaskInnerPage<InnerTaskT, SchemaT>> innerPage() {
      return () -> new CompositeTaskInnerPage<>(tasks, lastTaskAccumulator);
    }
  }
}
