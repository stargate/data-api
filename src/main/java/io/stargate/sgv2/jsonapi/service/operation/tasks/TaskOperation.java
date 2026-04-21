package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.TraceMessage;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObject;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An operation that executes a {@link TaskGroup} in a supplied {@link CommandContext}.
 *
 * @param <TaskT> The type of the task object that the operation is working with.
 * @param <SchemaT> The type of the schema object that the operation is working with.
 */
public class TaskOperation<TaskT extends Task<SchemaT>, SchemaT extends SchemaObject>
    implements Operation<SchemaT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TaskOperation.class);

  private final TaskGroup<TaskT, SchemaT> taskGroup;
  private final TaskAccumulator<TaskT, SchemaT> taskAccumulator;

  /**
   * Create a new {@link TaskOperation}
   *
   * @param taskGroup The tasks to run, grouped into a container that has config about how to run
   *     them as a group.
   * @param taskAccumulator The accumulator to send completed tasks to which can then provide a
   *     {@link CommandResult} from the tasks.
   */
  public TaskOperation(
      TaskGroup<TaskT, SchemaT> taskGroup, TaskAccumulator<TaskT, SchemaT> taskAccumulator) {

    this.taskGroup = Objects.requireNonNull(taskGroup, "taskGroup cannot be null");
    this.taskAccumulator =
        Objects.requireNonNull(taskAccumulator, "taskAccumulator cannot be null");
  }

  /**
   * Execute the tasks in the supplied context.
   *
   * <p>This is a generic operation that can be used to execute any type of tasks, the tasks are
   * executed using the configuration of the supplied {@link TaskGroup} and the {@link Task} itself.
   * The results are grouped using supplied {@link TaskAccumulator}, which creates the {@link
   * CommandResult}. Errors when executing the tasks are caught and attached to the {@link Task} so
   * they can be included in the {@link CommandResult}.
   *
   * @param commandContext The context to execute the tasks in.
   * @return A supplier of {@link CommandResult} that represents the result of running all the
   *     tasks.
   */
  @Override
  public Uni<Supplier<CommandResult>> execute(CommandContext<SchemaT> commandContext) {
    commandContext
        .requestTracing()
        .maybeTrace(
            () -> {
              var msg =
                  "Starting to process taskGroup of %s with status=%s"
                      .formatted(taskGroup.taskClassName(), taskGroup.statusCount());
              return new TraceMessage(msg, taskGroup);
            });

    return executeInternal(commandContext, TaskAccumulator::getResults)
        .invoke(
            () -> {
              commandContext
                  .requestTracing()
                  .maybeTrace(
                      () -> {
                        var msg =
                            "Completed processing taskGroup of %s with status=%s"
                                .formatted(taskGroup.taskClassName(), taskGroup.statusCount());
                        return new TraceMessage(msg, taskGroup);
                      });
            });
  }

  /**
   * Package internal overload that allows the result of running the tasks to be replaced with the
   * result of the supplied {@link Supplier}.
   *
   * <p>for use by the {@link CompositeTask}
   */
  <T> Uni<Supplier<T>> executeInternal(
      CommandContext<SchemaT> commandContext,
      Function<TaskAccumulator<TaskT, SchemaT>, Supplier<T>> resultSupplier) {

    Objects.requireNonNull(commandContext, "commandContext cannot be null");

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "executeInternal() - starting to process tasks={}", PrettyPrintable.print(taskGroup));
    }

    return startMulti(commandContext)
        .collect()
        .in(() -> taskAccumulator, TaskAccumulator::accumulate)
        .onItem()
        .invoke(
            () -> {
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "execute() - all task completed, taskGroup={}",
                    PrettyPrintable.print(taskGroup));
              }
            })
        .onItem()
        .invoke(taskGroup::throwIfNotAllTerminal)
        .onItem()
        .transform(resultSupplier);
  }

  /**
   * Start a {@link Multi} for processing the {@link #taskGroup}, the style of multi depends on the
   * {@link TaskGroup} configuration.
   *
   * @return A {@link Multi} that emits {@link TaskT} according to the configuration.
   */
  protected Multi<TaskT> startMulti(CommandContext<SchemaT> commandContext) {

    // Common start pattern for all operations
    var taskMulti = Multi.createFrom().iterable(taskGroup).onItem();

    if (taskGroup.getSequentialProcessing()) {
      // Tasks are processed sequentially. If one task fails, subsequent tasks should fail fast
      // without stopping the entire multi. Failures are recorded within the task objects
      // themselves,
      // rather than being emitted as failures by the multi stream.
      // (transformToUniAndConcatenate ensures sequential execution)

      return taskMulti.transformToUniAndConcatenate(
          task -> {
            var failFast = taskGroup.shouldFailFast(task);
            LOGGER.debug(
                "startMulti() - dequeuing task for sequential processing, failFast={}, task={}",
                failFast,
                task);

            if (failFast) {
              // Stop processing tasks, but we do not want to return a UniFailure, so we set the
              // tasks to skipped and do not call execute() on it.
              task.setSkippedIfReady();
              return Uni.createFrom().item(task);
            }
            return task.execute(commandContext);
          });
    }

    // Parallel processing using transformToUniAndMerge() - no extra testing.
    return taskMulti.transformToUniAndMerge(task -> task.execute(commandContext));
  }
}
