package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.stargate.sgv2.jsonapi.service.schema.SchemaObject;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.*;

/**
 * Ordered container of {@link Task} to be processed as a group.
 *
 * <p>The container can be configured to process the tasks sequentially or in parallel (default),
 * and other config that is needed about how to process the tasks as a group should live here (e.g.
 * if there is a delay between tasks, or if the container should fail fast).
 *
 * <p>TODO: aaron feb 4 - stop inheriting array list, just expose methods needed
 *
 * @param <SchemaT> Schema object type
 */
public class TaskGroup<TaskT extends Task<SchemaT>, SchemaT extends SchemaObject>
    extends ArrayList<TaskT> implements Recordable {

  private boolean sequentialProcessing = false;
  private final UUID groupId = UUID.randomUUID();

  public TaskGroup() {
    super();
  }

  /**
   * Initialize the container.
   *
   * @param sequentialProcessing If true the tasks will be processed sequentially, rather than in
   *     parallel, and the container will skip any remaining tasks if one fails. If false the tasks
   *     will be processed in parallel and all tasks will be processed regardless of the status of
   *     any other tasks.
   */
  public TaskGroup(boolean sequentialProcessing) {
    super();
    this.sequentialProcessing = sequentialProcessing;
  }

  public TaskGroup(TaskT task) {
    this(List.of(task));
  }

  public TaskGroup(List<TaskT> tasks) {
    super(tasks);
  }

  /**
   * @return Returns <code>true</code> if the container is configured for sequential processing.
   */
  public boolean getSequentialProcessing() {
    return sequentialProcessing;
  }

  public List<TaskT> errorTasks() {
    return stream().filter(task -> task.status() == Task.TaskStatus.ERROR).toList();
  }

  public boolean allTasksCompleted() {
    return stream().allMatch(task -> task.status() == Task.TaskStatus.COMPLETED);
  }

  public List<TaskT> completedTasks() {
    return stream().filter(task -> task.status() == Task.TaskStatus.COMPLETED).toList();
  }

  public List<TaskT> skippedTasks() {
    return stream().filter(task -> task.status() == Task.TaskStatus.SKIPPED).toList();
  }

  public void throwIfNotAllTerminal() {

    var nonTerminalTasks = stream().filter(task -> !task.status().isTerminal()).toList();

    if (!nonTerminalTasks.isEmpty()) {
      var msg = String.join(", ", nonTerminalTasks.stream().map(Object::toString).toList());
      throw new IllegalStateException(
          "throwIfNotAllTerminal() - Non terminal Tasks found, non-terminal=" + msg);
    }
  }

  /**
   * Determines whether the given {@code targetTask} should fail fast based on the container's
   * configuration and the current state of all tasks in this container.
   *
   * <p>If sequential processing is disabled, this method always returns {@code false} — all tasks
   * are allowed to run regardless of prior failures.
   *
   * <p>If sequential processing is enabled:
   *
   * <ul>
   *   <li>If the {@code targetTask} itself is in error state, it should fail fast.
   *   <li>If any task that comes <em>before</em> the {@code targetTask} is in error state, the
   *       {@code targetTask} should also fail fast.
   *   <li>Otherwise, the {@code targetTask} may proceed.
   * </ul>
   *
   * <p><b>Example:</b>
   *
   * <pre>
   * Suppose there are 5 tasks in order: a, b, c, d, e
   * Task c is in ERROR status.
   *
   * shouldFailFast(a) → false   (no errors before 'a', and 'a' is not in error)
   * shouldFailFast(b) → false   (no errors before 'b', and 'b' is not in error)
   * shouldFailFast(c) → true    ('c' itself is in error)
   * shouldFailFast(d) → true    ('c' failed earlier, so 'd' must fail fast)
   * shouldFailFast(e) → true    ('c' failed earlier, so 'e' must fail fast)
   * </pre>
   *
   * @param targetTask the task to evaluate
   * @return {@code true} if the container is configured for sequential processing and the {@code
   *     targetTask} (or any earlier task) is in error state; {@code false} otherwise
   */
  public boolean shouldFailFast(TaskT targetTask) {
    if (!sequentialProcessing) {
      return false;
    }
    // In sequential processing, if the target task is already in error state, we should fail fast
    if (targetTask.status() == BaseTask.TaskStatus.ERROR) {
      return true;
    }
    // In sequential processing, if any prior task is in error state, we should fail fast
    return stream()
        .takeWhile(task -> task != targetTask)
        .anyMatch(task -> task.status() == BaseTask.TaskStatus.ERROR);
  }

  @Override
  public String toString() {
    return "TaskGroup{"
        + "groupId="
        + groupId
        + ", sequentialProcessing="
        + sequentialProcessing
        + ", size="
        + size()
        + ", statusCount="
        + statusCount()
        + ", taskType="
        + taskClassName()
        + '}';
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    Map<BaseTask.TaskStatus, Integer> statusCount = new HashMap<>(size());
    forEach(task -> statusCount.merge(task.status(), 1, Math::addExact));

    return dataRecorder
        .append("groupId", groupId)
        .append("taskType", taskClassName())
        .append("sequentialProcessing", sequentialProcessing)
        .append("size", size())
        .append("statusCount", statusCount())
        .append("tasks", List.copyOf(this));
  }

  public Map<BaseTask.TaskStatus, Integer> statusCount() {
    Map<BaseTask.TaskStatus, Integer> statusCount = new HashMap<>(size());
    forEach(task -> statusCount.merge(task.status(), 1, Math::addExact));
    return statusCount;
  }

  public String taskClassName() {
    if (size() == 0) {
      return "<TaskListEmpty>";
    }
    var simpleName = get(0).getClass().getSimpleName();
    return simpleName.isBlank() ? get(0).getClass().getName() : simpleName;
  }
}
