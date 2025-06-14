package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Provides a base implementation for accumulating {@link Task}s that have completed processing by a
 * {@link TaskOperation}.
 *
 * <p>This base class provides the basic accumulate / getResults pattern. Subclasses can also be
 * used to "smuggle" state such as options for the response docs into the building of the results
 * via a subclass. the subclasses are used often called a PageBuilder see {@link TaskPage}
 */
public abstract class TaskAccumulator<TaskT extends Task<SchemaT>, SchemaT extends SchemaObject> {
  // TODO: remove all of error obj v2 flags, we use it all now
  protected boolean useErrorObjectV2 = false;
  protected RequestTracing requestTracing = null;

  protected final TaskGroup<TaskT, SchemaT> tasks = new TaskGroup<>();

  protected TaskAccumulator() {}

  public static <
          AccumT extends TaskAccumulator<TaskT, SchemaT>,
          TaskT extends Task<SchemaT>,
          SchemaT extends SchemaObject>
      AccumT configureForContext(AccumT accumulator, CommandContext<SchemaT> commandContext) {
    Objects.requireNonNull(accumulator, "accumulator cannot be null");
    Objects.requireNonNull(commandContext, "commandContext cannot be null");

    accumulator
        .useErrorObjectV2(commandContext.config().get(OperationsConfig.class).extendError())
        .requestTracing(commandContext.requestTracing());
    return accumulator;
  }

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
   * @return A supplier that will provide the {@link CommandResult} when called, such as a subclass
   *     of {@link TaskPage}
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
   * Set the {@link RequestTracing} object for the request. This will be passed to the {@link
   * io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder} which will add the tracing if
   * the object is not null and {@link RequestTracing#getTrace()} returns a trace.
   */
  @SuppressWarnings("unchecked")
  public <SubT extends TaskAccumulator<TaskT, SchemaT>> SubT requestTracing(
      RequestTracing requestTracing) {
    this.requestTracing = requestTracing;
    return (SubT) this;
  }
}
