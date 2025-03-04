package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;

/**
 * Hlpes build an {@link Operation} to run composite tasks of inner task groups.
 *
 * <p>There is some complex steps and relationships between the tasks, so this builder helps to
 * codeify them. See the {@link CompositeTask} for details on what it needs.
 *
 * <p>To use this builder:
 *
 * <ol>
 *   <li>The Composite Tasks created by the builder will run in a sequential task group.
 *   <li>Create task groups for the intermediate Composite Tasks, these are any composite task that
 *       is not the last. For each of these call {@link #withIntermediateTasks} with the retry
 *       policy. These tasks all use a {@link CompositeTaskInnerPage} which lifts erors and
 *       warnings, so you do not provide an accumulator.
 *   <li>Create the last group of tasks, it's retry policy, and a {@link TaskAccumulator} that will
 *       build the page of results from the last group of tasks. Pass these to {@link
 *       #build(TaskGroup, TaskRetryPolicy, TaskAccumulator)} to make the last Composite Tasks and
 *       chain it all together into an operation.
 * </ol>
 *
 * @param <SchemaT>
 */
public class CompositeTaskOperationBuilder<SchemaT extends SchemaObject>
    extends TaskBuilder<
        CompositeTask<?, SchemaT>, SchemaT, CompositeTaskOperationBuilder<SchemaT>> {

  // Always use sequential processing for composite tasks, the idea is the composite tasks can be
  // groups
  // of parallel tasks, but the composite task itself is always sequential
  private final TaskGroup<CompositeTask<?, SchemaT>, SchemaT> compositeTasks =
      new TaskGroup<>(true);

  private CommandContext<SchemaT> commandContext;

  public CompositeTaskOperationBuilder(CommandContext<SchemaT> commandContext) {
    super(commandContext.schemaObject());
    this.commandContext = commandContext;
  }

  /**
   * Call to add an intermediate task group of tasks, that is not the last ones.
   *
   * @param innerTasks The tasks for the Composite Task to run.
   * @param retryPolicy Retry policy when running the inner tasks.
   * @return This builder for chaining.
   * @param <InnerTaskT> The type of the inner tasks that will be run.
   */
  public <InnerTaskT extends Task<SchemaT>>
      CompositeTaskOperationBuilder<SchemaT> withIntermediateTasks(
          TaskGroup<InnerTaskT, SchemaT> innerTasks, TaskRetryPolicy retryPolicy) {

    CompositeTask<InnerTaskT, SchemaT> intermediaTask =
        CompositeTask.intermediateTask(nextPosition(), schemaObject, retryPolicy, innerTasks);
    compositeTasks.add(intermediaTask);
    return this;
  }

  /**
   * Call to build the last group of tasks to run, the builder will append these to the task groups
   * and create a new operation that will run the composite tasks.
   *
   * @param lastTaskInnerTasks The tasks for the last Composite Task to run.
   * @param lastTaskRetryPolicy Retry policy for the last task.
   * @param lastTaskAccumulator Accumulator to build the results of the last tasks, this will build
   *     the results of the operation.
   * @return The operation that will run the composite tasks.
   * @param <InnerTaskT> The type of the last inner tasks that will be run.
   */
  public <InnerTaskT extends Task<SchemaT>> Operation<SchemaT> build(
      TaskGroup<InnerTaskT, SchemaT> lastTaskInnerTasks,
      TaskRetryPolicy lastTaskRetryPolicy,
      TaskAccumulator<InnerTaskT, SchemaT> lastTaskAccumulator) {

    CompositeTask<InnerTaskT, SchemaT> lastTask =
        CompositeTask.lastTask(
            nextPosition(),
            schemaObject,
            lastTaskRetryPolicy,
            lastTaskInnerTasks,
            lastTaskAccumulator);

    compositeTasks.add(lastTask);

    CompositeTaskOuterPage.Accumulator<SchemaT> outerAccumulator =
        CompositeTaskOuterPage.accumulator(commandContext);

    return new TaskOperation<>(compositeTasks, outerAccumulator);
  }
}
