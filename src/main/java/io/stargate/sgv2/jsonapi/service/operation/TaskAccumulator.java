package io.stargate.sgv2.jsonapi.service.operation;

import io.smallrye.mutiny.Multi;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

import java.util.function.Supplier;

/**
 * Provides a base implementation for accumulating {@link OperationAttempt}s that the {@link
 * GenericOperation} calls when using hte {@link Multi#collect()} method.
 *
 * <p>This is here so the {@link GenericOperation} has a generic way to accumulate the attempts that
 * it will run, look at {@link OperationAttemptPageBuilder} for how to it is used.
 */
public abstract class TaskAccumulator<
    TaskT extends Task<SchemaT>,
    SchemaT extends SchemaObject> {

  protected boolean useErrorObjectV2 = false;
  protected boolean debugMode = false;

  protected final TaskGroup<TaskT, SchemaT> tasks =
      new TaskGroup<>();

  protected TaskAccumulator() {}

  public void accumulate(TaskT attempt) {
    tasks.add(attempt);
  }

  /**
   * Called ot turn the accumulated attempts into a {@link CommandResult}.
   *
   * @return A supplier that will provide the {@link CommandResult} when called, normally a sublcass
   *     of {@link OperationAttemptPage}
   */
  public abstract Supplier<CommandResult> getOperationPage();

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
  public <SubT extends TaskAccumulator<TaskT, SchemaT>> SubT debugMode(
      boolean debugMode) {
    this.debugMode = debugMode;
    return (SubT) this;
  }

}
