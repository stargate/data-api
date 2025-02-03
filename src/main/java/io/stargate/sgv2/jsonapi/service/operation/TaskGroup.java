package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

import java.util.*;

/**
 * Ordered container of {@link OperationAttempt} objects that are part of the same {@link
 * io.stargate.sgv2.jsonapi.api.model.command.Command}.
 *
 * <p>The container can be configured to process the attempts sequentially or in parallel (default),
 * and other config that is needed about how to process the attempts as a group should live here
 * (e.g. if there is a delay between attempts, or if the container should fail fast).
 *
 * @param <SchemaT> Schema object type
 * @param <AttemptT> Operation attempt type
 */
public class TaskGroup<
    TaskT extends Task<SchemaT>,
    SchemaT extends SchemaObject>
    extends ArrayList<TaskT> {

  private boolean sequentialProcessing = false;
  private final UUID containerId = UUID.randomUUID();

  public TaskGroup() {
    super();
  }

  /**
   * Initialize the container.
   *
   * @param sequentialProcessing If true the attempts will be processed sequentially, rather than in
   *     parallel, and the container will skip any remaining attempts if one fails. If false the
   *     attempts will be processed in parallel and all attempts will be processed regardless of the
   *     status of any other attempts.
   */
  public TaskGroup(boolean sequentialProcessing) {
    super();
    this.sequentialProcessing = sequentialProcessing;
  }

  public TaskGroup(TaskT attempt) {
    this(List.of(attempt));
  }

  public TaskGroup(List<TaskT> attempts) {
    super(attempts);
  }

  /**
   * See {@link #OperationAttemptContainer(boolean)}.
   *
   * @return If the container will process the attempts sequentially.
   */
  public boolean getSequentialProcessing() {
    return sequentialProcessing;
  }


  public List<TaskT> errorTasks() {
    return stream()
        .filter(task -> task.status() == BaseTask.TaskStatus.ERROR)
        .toList();
  }

  public boolean allTasksCompleted() {
    return stream()
        .allMatch(task -> task.status() == BaseTask.TaskStatus.COMPLETED);
  }

  public List<TaskT> completedTasks() {
    return stream()
        .filter(task -> task.status() == BaseTask.TaskStatus.COMPLETED)
        .toList();
  }

  public List<TaskT> skippedTasks() {
    return stream()
        .filter(task -> task.status() == BaseTask.TaskStatus.SKIPPED)
        .toList();
  }

  public void throwIfNotAllTerminal() {

    var nonTerminalAttempts = stream().filter(task -> !task.status().isTerminal()).toList();

    if (!nonTerminalAttempts.isEmpty()) {
      var msg = String.join(", ", nonTerminalAttempts.stream().map(Object::toString).toList());
      throw new IllegalStateException(
          "throwIfNotAllTerminal() - Non terminal Tasks found, non-terminal=" + msg);
    }
  }

  /**
   * Checks if, given the config of the container and the current state of the attempts, the
   * container should fail fast and stop processing any further attempts.
   *
   * @return <code>true</code> if the container is configured for sequential processing and there is
   *     at least one error
   */
  public boolean shouldFailFast() {
    if (!sequentialProcessing) {
      return false;
    }
    return stream().anyMatch(task -> task.status() == BaseTask.TaskStatus.ERROR);
  }

  @Override
  public String toString() {
    Map<BaseTask.TaskStatus, Integer> statusCount = new HashMap<>(size());
    forEach(attempt -> statusCount.merge(attempt.status(), 1, Math::addExact));

    return new StringBuilder("TaskGroup{")
        .append("containerId=")
        .append(containerId)
        .append(", sequentialProcessing=")
        .append(sequentialProcessing)
        .append(", count=")
        .append(size())
        .append(", statusCount=")
        .append(statusCount)
        .append('}')
        .toString();
  }
}
