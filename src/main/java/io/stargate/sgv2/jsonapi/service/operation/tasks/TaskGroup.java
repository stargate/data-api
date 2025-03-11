package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
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
    return stream().filter(task -> task.status() == BaseTask.TaskStatus.ERROR).toList();
  }

  public boolean allTasksCompleted() {
    return stream().allMatch(task -> task.status() == BaseTask.TaskStatus.COMPLETED);
  }

  public List<TaskT> completedTasks() {
    return stream().filter(task -> task.status() == BaseTask.TaskStatus.COMPLETED).toList();
  }

  public List<TaskT> skippedTasks() {
    return stream().filter(task -> task.status() == BaseTask.TaskStatus.SKIPPED).toList();
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
   * Checks if, given the config of the container and the current state of the tasks, the container
   * should fail fast and stop processing any further tasks.
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

  //  @Override
  //  public String toString() {
  //    Map<BaseTask.TaskStatus, Integer> statusCount = new HashMap<>(size());
  //    forEach(task -> statusCount.merge(task.status(), 1, Math::addExact));
  //
  //    return new StringBuilder("TaskGroup{")
  //        .append("groupId=")
  //        .append(groupId)
  //        .append(", taskType=")
  //        .append(size() > 0 ? get(0).getClass().getSimpleName() : "<TaskListEmpty>")
  //        .append(", sequentialProcessing=")
  //        .append(sequentialProcessing)
  //        .append(", count=")
  //        .append(size())
  //        .append(", statusCount=")
  //        .append(statusCount)
  //        .append('}')
  //        .toString();
  //  }

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
