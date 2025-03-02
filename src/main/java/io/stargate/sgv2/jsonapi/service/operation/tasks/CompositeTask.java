package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Task that runs an operation */
public class CompositeTask<InnerTaskT extends Task<SchemaT>, SchemaT extends SchemaObject>
    extends BaseTask<
        SchemaT,
        CompositeTask.CompositeTaskResultSupplier<InnerTaskT, SchemaT>,
        CompositeTaskIntermediatePage<InnerTaskT, SchemaT>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CompositeTask.class);

  //  private final Class<InnerTaskT> taskClass;
  private final TaskGroup<InnerTaskT, SchemaT> innerTaskGroup;

  /**
   * Accumulator set if this is the task CompositeTask, this is the regular accumulator we would use
   * with the InnerT
   */
  private final TaskAccumulator<InnerTaskT, SchemaT> lastTaskAccumulator;

  private CompositeTaskIntermediatePage<InnerTaskT, SchemaT> innerPage;

  public CompositeTask(
      int position,
      SchemaT schemaObject,
      TaskRetryPolicy retryPolicy,
      TaskGroup<InnerTaskT, SchemaT> innerTaskGroup,
      TaskAccumulator<InnerTaskT, SchemaT> lastTaskAccumulator) {
    super(position, schemaObject, retryPolicy);

    this.innerTaskGroup = innerTaskGroup;
    this.lastTaskAccumulator = lastTaskAccumulator;
    setStatus(TaskStatus.READY);
  }

  public static <InnerTaskT extends Task<SchemaT>, SchemaT extends SchemaObject>
      CompositeTask<InnerTaskT, SchemaT> intermediateTask(
          int position,
          SchemaT schemaObject,
          TaskRetryPolicy retryPolicy,
          TaskGroup<InnerTaskT, SchemaT> taskGroup) {

    return new CompositeTask<>(position, schemaObject, retryPolicy, taskGroup, null);
  }

  public static <InnerTaskT extends Task<SchemaT>, SchemaT extends SchemaObject>
      CompositeTask<InnerTaskT, SchemaT> lastTask(
          int position,
          SchemaT schemaObject,
          TaskRetryPolicy retryPolicy,
          TaskGroup<InnerTaskT, SchemaT> taskGroup,
          TaskAccumulator<InnerTaskT, SchemaT> taskAccumulator) {

    return new CompositeTask<>(position, schemaObject, retryPolicy, taskGroup, taskAccumulator);
  }

  @Override
  protected CompositeTaskResultSupplier<InnerTaskT, SchemaT> buildResultSupplier(
      CommandContext<SchemaT> commandContext) {

    // We always need the intermediate accumulator
    // If this is an intermedia (i.e. not last) task then it will be used to surface errors from the
    // inner tasks
    // if this is the last task, it will be used to pass through to the lastTaskAccumulator
    // which is the accumilator to make the results of the entire pipeline
    var intermediateAccumulator =
        CompositeTaskIntermediatePage.<InnerTaskT, SchemaT>accumulator(commandContext)
            .withLastTaskAccumulator(lastTaskAccumulator);

    // we give the Operation either the lastTaskAccumulator if we have one, or the intermedia one
    // the result supplier below will call the correct accumulator when done
    TaskAccumulator<InnerTaskT, SchemaT> operationAccumulator =
        lastTaskAccumulator != null ? lastTaskAccumulator : intermediateAccumulator;

    TaskOperation<InnerTaskT, SchemaT> innerOperation =
        new TaskOperation<>(innerTaskGroup, operationAccumulator);

    return new CompositeTaskResultSupplier<InnerTaskT, SchemaT>(
        this, commandContext, innerOperation, intermediateAccumulator, lastTaskAccumulator);
  }

  @Override
  protected RuntimeException maybeHandleException(
      CompositeTaskResultSupplier<InnerTaskT, SchemaT> resultSupplier,
      RuntimeException runtimeException) {
    return null;
  }

  @Override
  protected void onSuccess(CompositeTaskIntermediatePage<InnerTaskT, SchemaT> result) {
    innerPage = result;
    super.onSuccess(result);
  }

  @Override
  public Task<SchemaT> setSkippedIfReady() {
    // make sure we pass this though to the inner tasks, the CompositeTask has been skipped
    // so all inner tasks are also skipped
    innerTaskGroup.forEach(Task::setSkippedIfReady);
    return super.setSkippedIfReady();
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return super.recordTo(dataRecorder)
        .append("innerTaskGroup", innerTaskGroup)
        .append("lastTaskAccumulator", lastTaskAccumulator);
  }

  TaskAccumulator<InnerTaskT, SchemaT> lastTaskAccumulator() {
    return lastTaskAccumulator;
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
        innerTaskGroup.stream().map(Task::allWarnings).flatMap(List::stream).toList());

    return allWarnings;
  }

  @Override
  public List<WarningException> warningsExcludingSuppressed() {

    var allWarnings = new ArrayList<>(super.warningsExcludingSuppressed());
    allWarnings.addAll(
        innerTaskGroup.stream()
            .map(Task::warningsExcludingSuppressed)
            .flatMap(List::stream)
            .toList());

    return allWarnings;
  }

  /**
   * NOTE: Need to keep this class static, the explicit generics are needed for the compiler.
   *
   * @param <InnerTaskT>
   * @param <SchemaT>
   */
  public static class CompositeTaskResultSupplier<
          InnerTaskT extends Task<SchemaT>, SchemaT extends SchemaObject>
      implements BaseTask.UniSupplier<CompositeTaskIntermediatePage<InnerTaskT, SchemaT>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompositeTask.class);

    private final Task<SchemaT> compositeTask;
    private final TaskOperation<InnerTaskT, SchemaT> innerOperation;
    private final CommandContext<SchemaT> commandContext;
    private final CompositeTaskIntermediatePage.Accumulator<InnerTaskT, SchemaT>
        intermediateAccumulator;
    TaskAccumulator<InnerTaskT, SchemaT> lastAccumulator;

    public CompositeTaskResultSupplier(
        Task<SchemaT> compositeTask,
        CommandContext<SchemaT> commandContext,
        TaskOperation<InnerTaskT, SchemaT> innerOperation,
        CompositeTaskIntermediatePage.Accumulator<InnerTaskT, SchemaT> intermediateAccumulator,
        TaskAccumulator<InnerTaskT, SchemaT> lastAccumulator) {
      this.compositeTask = compositeTask;
      this.innerOperation = innerOperation;
      this.commandContext = commandContext;
      this.intermediateAccumulator = intermediateAccumulator;
      this.lastAccumulator = lastAccumulator;
    }

    @Override
    public Uni<CompositeTaskIntermediatePage<InnerTaskT, SchemaT>> get() {

      return innerOperation
          .executeInternal(
              commandContext, accumulator -> intermediateAccumulator.intermediatePage())
          .onItem()
          .transform(Supplier::get)
          .invoke(
              innerPage -> {
                if (lastAccumulator != null) {
                  innerPage.fetchLastTaskResults();
                } else {
                  // we could throw an exception here, and the task processing would catch and
                  // associate with
                  // the task. But tasks can fail for Throwable exceptions, quarkus just hands them
                  // in, and
                  // we cannot throw that. So pass the inner task error up to the composite task,
                  // this
                  // wil set the status to failure.
                  innerPage
                      .firstFailedTask()
                      .ifPresent(
                          task -> {
                            var failure = task.failure().orElseThrow();
                            if (LOGGER.isDebugEnabled()) {
                              LOGGER.debug(
                                  "get() - Lifting failure from inner task to composite task, innerTask: {}",
                                  task.taskDesc(),
                                  failure);
                            }
                            compositeTask.maybeAddFailure(failure);
                          });
                }
              });
    }
  }
}
