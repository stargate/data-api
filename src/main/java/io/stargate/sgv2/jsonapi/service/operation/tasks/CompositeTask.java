package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Composite tasks that runs a group of inner tasks, wrapped in an inner operation.
 *
 * <p>Because we always want to create Composite Tasks in a group of sequential tasks, and the
 * building is complex use the {@link CompositeTaskOperationBuilder}.
 *
 * <p>The task creates an operation to run the inner tasks, and then uses the {@link
 * CompositeTaskInnerPage} to accumulate the results. This will pull through errors and warnings
 * from the inner tasks, so they appear as the result of running this task.
 *
 * <p>The last Composite Task in a group will also have the {@link TaskAccumulator} that is normally
 * used with the inner tasks, so it can build the final result of running all the tasks.
 *
 * <p>
 *
 * @param <InnerTaskT> The type of the inner task that will be run.
 * @param <SchemaT> The schema object type.
 */
public class CompositeTask<InnerTaskT extends Task<SchemaT>, SchemaT extends SchemaObject>
    extends BaseTask<
        SchemaT,
        CompositeTask.CompositeTaskResultSupplier<InnerTaskT, SchemaT>,
        CompositeTaskInnerPage<InnerTaskT, SchemaT>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CompositeTask.class);

  private final TaskGroup<InnerTaskT, SchemaT> innerTaskGroup;

  /**
   * Accumulator set if this is the task CompositeTask, this is the regular accumulator we would use
   * with the InnerT
   */
  private final TaskAccumulator<InnerTaskT, SchemaT> lastTaskAccumulator;

  // TODO: I think this can be removed - aaron
  private CompositeTaskInnerPage<InnerTaskT, SchemaT> innerPage;

  /**
   * Create a new CompositeTask.
   *
   * @param position The position of this task in the operation.
   * @param schemaObject The schema object for this task.
   * @param retryPolicy The retry policy to use with this composite task, inner tasks have their own
   *     retry policy.
   * @param innerTaskGroup The group of inner tasks to run.
   * @param lastTaskAccumulator Nullable accumulator to use if this is the last task in the group.
   */
  public CompositeTask(
      int position,
      SchemaT schemaObject,
      TaskRetryPolicy retryPolicy,
      TaskGroup<InnerTaskT, SchemaT> innerTaskGroup,
      TaskAccumulator<InnerTaskT, SchemaT> lastTaskAccumulator) {
    super(position, schemaObject, retryPolicy);

    this.innerTaskGroup = Objects.requireNonNull(innerTaskGroup, "innerTaskGroup cannot be null");
    if (innerTaskGroup.tasks().isEmpty()) {
      throw new IllegalArgumentException("innerTaskGroup cannot be empty");
    }
    // last task accumulator can be null, if this is an intermediate task
    this.lastTaskAccumulator = lastTaskAccumulator;
    setStatus(TaskStatus.READY);
  }

  /**
   * Factory to create a new CompositeTask that is an intermediate task, i.e. not the last task in a
   * group.
   *
   * @param position The position of this task in the operation.
   * @param schemaObject The schema object for this task.
   * @param retryPolicy The retry policy to use with this composite task, inner tasks have their own
   *     retry policy.
   * @param innerTaskGroup The inner tasks that should be run.
   * @return A new CompositeTask.
   * @param <InnerTaskT> The type of the inner tasks the composite task will run.
   * @param <SchemaT> The schema object type.
   */
  public static <InnerTaskT extends Task<SchemaT>, SchemaT extends SchemaObject>
      CompositeTask<InnerTaskT, SchemaT> intermediateTask(
          int position,
          SchemaT schemaObject,
          TaskRetryPolicy retryPolicy,
          TaskGroup<InnerTaskT, SchemaT> innerTaskGroup) {

    return new CompositeTask<>(position, schemaObject, retryPolicy, innerTaskGroup, null);
  }

  /**
   * Factory to create a new CompositeTask that is the last task in a group. This task needs the
   * {@link TaskAccumulator} to build the final result of the group of tasks.
   *
   * @param position The position of this task in the operation.
   * @param schemaObject The schema object for this task.
   * @param retryPolicy The retry policy to use with this composite task, inner tasks have their own
   *     retry policy.
   * @param innerTaskGroup The inner tasks that should be run.
   * @param taskAccumulator The accumulator to create the results of the entire group of tasks, this
   *     is normally the accumulator you would use with the inner tasks, e.g. a {@link
   *     io.stargate.sgv2.jsonapi.service.operation.InsertDBTaskPage}
   * @return A new CompositeTask.
   * @param <InnerTaskT> The type of the inner tasks the composite task will run.
   * @param <SchemaT> The schema object type.
   */
  public static <InnerTaskT extends Task<SchemaT>, SchemaT extends SchemaObject>
      CompositeTask<InnerTaskT, SchemaT> lastTask(
          int position,
          SchemaT schemaObject,
          TaskRetryPolicy retryPolicy,
          TaskGroup<InnerTaskT, SchemaT> innerTaskGroup,
          TaskAccumulator<InnerTaskT, SchemaT> taskAccumulator) {

    return new CompositeTask<>(
        position, schemaObject, retryPolicy, innerTaskGroup, taskAccumulator);
  }

  // ==================================================================================================================
  // BaseTask Overrides
  // ==================================================================================================================

  @Override
  protected CompositeTaskResultSupplier<InnerTaskT, SchemaT> buildResultSupplier(
      CommandContext<SchemaT> commandContext) {

    // We always need the inner accumulator to collection the results of the inner tasks and build
    // the
    // inner page we use for the result of this composite tasks.
    // It is used to lift errors and warnings from the inner tasks, so they are associated with this
    // task.
    // and if this is the last task, it will be used to pass through to the lastTaskAccumulator
    // which is the accumulator to make the results of the entire pipeline
    var innerAccumulator =
        CompositeTaskInnerPage.<InnerTaskT, SchemaT>accumulator(commandContext)
            .withLastTaskAccumulator(lastTaskAccumulator);

    // we can use the innerAccumulator with the tasks even if, it will pass through to the
    // lastTaskAccumulator if there is one so we can get the results of the entire pipeline
    TaskOperation<InnerTaskT, SchemaT> innerOperation =
        new TaskOperation<>(innerTaskGroup, innerAccumulator);

    return new CompositeTaskResultSupplier<>(
        this, commandContext, innerOperation, innerAccumulator);
  }

  @Override
  protected RuntimeException maybeHandleException(
      CompositeTaskResultSupplier<InnerTaskT, SchemaT> resultSupplier,
      RuntimeException runtimeException) {
    // return the same error, so it will be associated with the task
    return runtimeException;
  }

  @Override
  protected void onSuccess(CompositeTaskInnerPage<InnerTaskT, SchemaT> result) {
    // TODO: Aaron, - not sure we need to do this ?
    innerPage = result;
    super.onSuccess(result);
  }

  @Override
  public Task<SchemaT> setSkippedIfReady() {
    // make sure we pass this though to the inner tasks, the CompositeTask has been skipped
    // so all inner tasks are also skipped
    innerTaskGroup.tasks().forEach(Task::setSkippedIfReady);
    return super.setSkippedIfReady();
  }

  // ===================================================================================================================
  // WarningsSink Overrides
  // ===================================================================================================================

  /**
   * Returns the warnings from this CompositeTask appended with any warnings from the inner tasks.
   */
  @Override
  public List<WarningException> allWarnings() {

    var allWarnings = new ArrayList<>(super.allWarnings());
    allWarnings.addAll(
        innerTaskGroup.tasks().stream().map(Task::allWarnings).flatMap(List::stream).toList());

    return allWarnings;
  }

  /**
   * Returns the warnings from this CompositeTask appended with any warnings from the inner tasks,
   *
   * @return
   */
  @Override
  public List<WarningException> warningsExcludingSuppressed() {

    var allWarnings = new ArrayList<>(super.warningsExcludingSuppressed());
    allWarnings.addAll(
        innerTaskGroup.tasks().stream()
            .map(Task::warningsExcludingSuppressed)
            .flatMap(List::stream)
            .toList());

    return allWarnings;
  }

  // ===================================================================================================================
  // Implementation and internals
  // ===================================================================================================================

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return super.recordTo(dataRecorder)
        .append("innerTaskGroup", innerTaskGroup)
        .append("lastTaskAccumulator", lastTaskAccumulator);
  }

  /**
   * If we are the last task, then the {@link CompositeTaskOuterPage} will call this to get the
   * accumulator from the inner operation to get the result for the entire operation.
   */
  TaskAccumulator<InnerTaskT, SchemaT> lastTaskAccumulator() {
    Objects.requireNonNull(
        lastTaskAccumulator,
        "lastTaskAccumulator called on a task that is not the last task, " + taskDesc());
    return lastTaskAccumulator;
  }

  /**
   * Runs the inner tasks in an inner operation, and accumulates their results into a {@link
   * CompositeTaskInnerPage} that holds the inner tasks we have run.
   *
   * <p>NOTE: Need to keep this class static, the explicit generics are needed for the compiler.
   *
   * @param <InnerTaskT>
   * @param <SchemaT>
   */
  public static class CompositeTaskResultSupplier<
          InnerTaskT extends Task<SchemaT>, SchemaT extends SchemaObject>
      implements BaseTask.UniSupplier<CompositeTaskInnerPage<InnerTaskT, SchemaT>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompositeTask.class);

    private final Task<SchemaT> compositeTask;
    private final CommandContext<SchemaT> commandContext;
    private final TaskOperation<InnerTaskT, SchemaT> innerOperation;
    private final CompositeTaskInnerPage.Accumulator<InnerTaskT, SchemaT> innerAccumulator;

    public CompositeTaskResultSupplier(
        Task<SchemaT> compositeTask,
        CommandContext<SchemaT> commandContext,
        TaskOperation<InnerTaskT, SchemaT> innerOperation,
        CompositeTaskInnerPage.Accumulator<InnerTaskT, SchemaT> innerAccumulator) {
      this.compositeTask = compositeTask;
      this.commandContext = commandContext;
      this.innerOperation = innerOperation;
      this.innerAccumulator = innerAccumulator;
    }

    @Override
    public Uni<CompositeTaskInnerPage<InnerTaskT, SchemaT>> get() {

      // Operations return a CommandResult, but for the CompositeTask we need to use the
      // CompositeTakeInnerPage as the result of running the operation so we can list errors and
      // results from the
      // inner tasks. So use the executeIternal to run the operation but control the result type
      return innerOperation
          .executeInternal(commandContext, accumulator -> innerAccumulator.innerPage())
          .onItem()
          .transform(Supplier::get)
          .invoke(
              innerPage -> {
                // we could throw an exception here, and the task processing would catch and
                // associate with the task. But tasks can fail for Throwable exceptions,
                // quarkus just hands them in, and we cannot throw that. So lift the inner task
                // error
                // up to the composite task, which will set the status to failure.
                innerPage
                    .firstFailedTask()
                    .ifPresent(
                        task -> {
                          var failure = task.failure().orElseThrow();
                          if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(
                                "get() - Lifting failure from inner task to composite task, innerTask: {}, compositeTask: {}",
                                task.taskDesc(),
                                compositeTask.taskDesc(),
                                failure);
                          }
                          compositeTask.maybeAddFailure(failure);
                        });
              });
    }
  }
}
