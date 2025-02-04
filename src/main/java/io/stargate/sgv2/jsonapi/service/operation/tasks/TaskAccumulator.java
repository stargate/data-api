package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Provides a base implementation for accumulating {@link Task}s that have completed processing by a
 * {@link TaskOperation}.
 *
 * <p>
 *  This base class provides the basic accumulate / getResults pattern. Subclasses can also be used to
 *  "smuggle" state such as options for the response docs into the building of the results via a subclass. the
 *  subclasses are used often called a PageBuilder see {@link DBTaskPage}
 */
public abstract class TaskAccumulator<TaskT extends Task<SchemaT>, SchemaT extends SchemaObject> {

  // TODO: remove all of error obj v2 flags, we use it all now
  protected boolean useErrorObjectV2 = false;
  protected boolean debugMode = false;

  protected final TaskGroup<TaskT, SchemaT> tasks = new TaskGroup<>();

  protected TaskAccumulator() {}

  /**
   * Called for each task that has completed processing.
   *
   * @param task The task that has completed processing.
   */
  public void accumulate(TaskT task) {
    tasks.add(Objects.requireNonNull(task, "task cannot be null"));
  }

  /**
   * Called to turn the accumulated attempts into a {@link CommandResult}.
   *
   * @return A supplier that will provide the {@link CommandResult} when called, such as a subclass of
   *     {@link DBTaskPage}
   */
  public abstract Supplier<CommandResult> getResults();

  /**
   * Sets if the error object v2 formatting should be used when building the {@link CommandResult}.
   */
  @SuppressWarnings("unchecked")
  public <SubT extends TaskAccumulator<TaskT, SchemaT>> SubT useErrorObjectV2(
      boolean useErrorObjectV2) {
    this.useErrorObjectV2 = useErrorObjectV2;
    return (SubT) this;
  }

  /**
   * Set if API is running in debug mode, this adds additional info to the response. See {@link
   * io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder}.
   */
  @SuppressWarnings("unchecked")
  public <SubT extends TaskAccumulator<TaskT, SchemaT>> SubT debugMode(boolean debugMode) {
    this.debugMode = debugMode;
    return (SubT) this;
  }
}
