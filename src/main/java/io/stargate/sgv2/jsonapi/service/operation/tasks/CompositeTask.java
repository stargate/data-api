package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.function.Supplier;

/** Task that runs an operation */
public class CompositeTask<InnerTaskT extends Task<SchemaT>, SchemaT extends SchemaObject>
    extends BaseTask<
        SchemaT,
        CompositeTask.CompositeTaskResultSupplier<InnerTaskT, SchemaT>,
        CompositeTaskIntermediatePage<InnerTaskT, SchemaT>> {

  //  private final Class<InnerTaskT> taskClass;
  private final TaskGroup<InnerTaskT, SchemaT> taskGroup;

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
      TaskGroup<InnerTaskT, SchemaT> taskGroup,
      TaskAccumulator<InnerTaskT, SchemaT> lastTaskAccumulator) {
    super(position, schemaObject, retryPolicy);

    this.taskGroup = taskGroup;
    this.lastTaskAccumulator = lastTaskAccumulator;
    setStatus(TaskStatus.READY);
  }

  public static <InnerTaskT extends Task<SchemaT>, SchemaT extends SchemaObject>
      CompositeTask<InnerTaskT, SchemaT> intermediaTask(
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
    // If this is an intermedia (i.e. not last) task then it will be used to surface errors from the inner tasks
    // if this is the last task, it will be used to pass through to the lastTaskAccumulator
    // which is the accumilator to make the results of the entire pipeline
    var intermediateAccumulator = CompositeTaskIntermediatePage.<InnerTaskT, SchemaT>accumulator(commandContext)
        .withLastTaskAccumulator(lastTaskAccumulator);

    // we give the Operation either the lastTaskAccumulator if we have one, or the intermedia one
    // the result supplier below will call the correct accumulator when done
    TaskAccumulator<InnerTaskT, SchemaT> operationAccumulator =
        lastTaskAccumulator != null ? lastTaskAccumulator : intermediateAccumulator;

    TaskOperation<InnerTaskT, SchemaT> innerOperation =
        new TaskOperation<>(taskGroup, operationAccumulator);

    return new CompositeTaskResultSupplier<InnerTaskT, SchemaT>(
        commandContext, innerOperation, intermediateAccumulator, lastTaskAccumulator);
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

  CompositeTaskIntermediatePage<InnerTaskT, SchemaT> innerPage() {
    return innerPage;
  }

  public static class CompositeTaskResultSupplier<
          InnerTaskT extends Task<SchemaT>, SchemaT extends SchemaObject>
      implements BaseTask.UniSupplier<CompositeTaskIntermediatePage<InnerTaskT, SchemaT>> {

    private final TaskOperation<InnerTaskT, SchemaT> innerOperation;
    private final CommandContext<SchemaT> commandContext;
    private final CompositeTaskIntermediatePage.Accumulator<InnerTaskT, SchemaT>
        intermediateAccumulator;
    TaskAccumulator<InnerTaskT, SchemaT> lastAccumulator;

    public CompositeTaskResultSupplier(
        CommandContext<SchemaT> commandContext,
        TaskOperation<InnerTaskT, SchemaT> innerOperation,
        CompositeTaskIntermediatePage.Accumulator<InnerTaskT, SchemaT> intermediateAccumulator,
        TaskAccumulator<InnerTaskT, SchemaT> lastAccumulator) {
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
                  innerPage.throwIfErrors();
                }
              });
    }
  }
}
