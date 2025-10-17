package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.util.ClassUtils;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.*;

/**
 * Ordered container of {@link Task} to be processed as a group.
 *
 * <p>The TaskGroup can be configured to process the tasks sequentially or in parallel (default),
 * and other config that is needed about how to process the tasks as a group should live here (e.g.
 * if there is a delay between tasks, or if the TaskGroup should fail fast). See {@link
 * #shouldFailFast(TaskT)}
 *
 * @param <SchemaT> Schema object type
 */
public class TaskGroup<TaskT extends Task<SchemaT>, SchemaT extends SchemaObject>
    implements Recordable {

  private static final boolean DEFAULT_SEQUENTIAL_PROCESSING = false;

  // NOTE: this should only be accessed in functions design to mutate the list, e.g. add()
  // because we must guarantee Task.position() ordering.
  // use tasksView for all other access
  private final List<TaskT> mutableTasks;
  // unmodifiable view of the tasks, that can be shared outside the class
  private final List<TaskT> tasksView;

  private final boolean sequentialProcessing;
  private final UUID groupId = UUID.randomUUID();

  private TaskGroup(boolean sequentialProcessing, int taskCapacity) {
    this.sequentialProcessing = sequentialProcessing;
    this.mutableTasks = new ArrayList<>(taskCapacity);
    this.tasksView = Collections.unmodifiableList(mutableTasks);
  }

  /** Initialize the TaskGroup with parallel processing. */
  public TaskGroup() {
    this(DEFAULT_SEQUENTIAL_PROCESSING);
  }

  /**
   * Initialize the TaskGroup.
   *
   * @param sequentialProcessing If true the tasks will be processed sequentially, rather than in
   *     parallel, and the TaskGroup will skip any remaining tasks if one fails. If false the tasks
   *     will be processed in parallel and all tasks will be processed regardless of the status of
   *     any other tasks. See {@link #shouldFailFast(TaskT)}
   */
  public TaskGroup(boolean sequentialProcessing) {
    this(sequentialProcessing, 1);
    // Reads and  insertOne will normally only have a single task, optimize for that.
  }

  /**
   * Initialize the TaskGroup with a single task, using parallel processing.
   *
   * @param task
   */
  public TaskGroup(TaskT task) {
    this(List.of(Objects.requireNonNull(task, "task cannot be null")));
  }

  /**
   * Initialize the TaskGroup with a list of tasks, using parallel processing.
   *
   * @param tasks Tasks to add to the group
   */
  public TaskGroup(List<TaskT> tasks) {
    this(
        DEFAULT_SEQUENTIAL_PROCESSING,
        Objects.requireNonNull(tasks, "tasks cannot be null").size());
    tasks.forEach(this::add);
  }

  /**
   * Gets an unmodifiable list of the tasks in the TaskGroup, order is ascending {@link
   * Task#position()}
   *
   * @return Unmodifiable list of tasks
   */
  public List<TaskT> tasks() {
    return tasksView;
  }

  /**
   * Adds the supplied task to the group, in ascending {@link Task#position()} order
   *
   * @param task The task to add
   */
  public void add(TaskT task) {
    Objects.requireNonNull(task, "task cannot be null");

    // Task is Comparable
    int index = Collections.binarySearch(mutableTasks, task, Comparator.naturalOrder());
    if (index >= 0) {
      throw new IllegalArgumentException(
          "Existing task in TaskGroup with the same position. task=" + task.taskDesc());
    }

    int insertionPoint = -index - 1;
    mutableTasks.add(insertionPoint, task);
  }

  public void addAll(List<TaskT> tasksToAdd) {
    Objects.requireNonNull(tasksToAdd, "tasksToAdd cannot be null");
    tasksToAdd.forEach(this::add);
  }

  //
  //  public int size() {
  //    return unmodifiableTaskView.size();
  //  }
  //
  //  public boolean isEmpty(){
  //    return unmodifiableTaskView.isEmpty();
  //  }
  //
  //  /** Gets an unmodifiable iterator of the tasks in the TaskGroup */
  //  @Override
  //  public Iterator<TaskT> iterator() {
  //    return unmodifiableTaskView.iterator();
  //  }
  //
  //  /** Gets an unmodifiable stream of the tasks in the TaskGroup */
  //  public Stream<TaskT> stream() {
  //    return unmodifiableTaskView.stream();
  //  }

  /**
   * @return Returns <code>true</code> if the TaskGroup is configured for sequential processing.
   */
  public boolean getSequentialProcessing() {
    return sequentialProcessing;
  }

  public List<TaskT> errorTasks() {
    return tasksView.stream().filter(task -> task.status() == Task.TaskStatus.ERROR).toList();
  }

  public boolean allTasksCompleted() {
    return tasksView.stream().allMatch(task -> task.status() == Task.TaskStatus.COMPLETED);
  }

  public List<TaskT> completedTasks() {
    return tasksView.stream().filter(task -> task.status() == Task.TaskStatus.COMPLETED).toList();
  }

  public List<TaskT> skippedTasks() {
    return tasksView.stream().filter(task -> task.status() == Task.TaskStatus.SKIPPED).toList();
  }

  public void throwIfNotAllTerminal() {

    var nonTerminalTasks = tasksView.stream().filter(task -> !task.status().isTerminal()).toList();

    if (!nonTerminalTasks.isEmpty()) {
      var msg = String.join(", ", nonTerminalTasks.stream().map(Task::taskDesc).toList());
      throw new IllegalStateException(
          "throwIfNotAllTerminal() - Non terminal Tasks found, non-terminal=" + msg);
    }
  }

  /**
   * Determines whether the given {@code targetTask} should fail fast based on the TaskGroup's
   * configuration and the current state of all tasks in this TaskGroup.
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
   * @param candidateTask the next candidate task to be processed
   * @return {@code true} if the {@code candidateTask} should fail fast, that is it should be
   *     skipped without execution; {@code false} otherwise.
   */
  public boolean shouldFailFast(TaskT candidateTask) {
    Objects.requireNonNull(candidateTask, "candidateTask cannot be null");

    if (!sequentialProcessing) {
      return false;
    }

    // In sequential processing, if the target task is already in error state, we should fail fast
    if (candidateTask.status() == BaseTask.TaskStatus.ERROR) {
      return true;
    }
    // In sequential processing, if any prior task is in error state, we should fail fast
    // array order is maintained as position() order by the add() method
    return tasksView.stream()
        .takeWhile(task -> task.position() < candidateTask.position())
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
        + tasksView.size()
        + ", statusCount="
        + statusCount()
        + ", taskType="
        + taskClassName()
        + '}';
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return dataRecorder
        .append("groupId", groupId)
        .append("taskType", taskClassName())
        .append("sequentialProcessing", sequentialProcessing)
        .append("size", tasksView.size())
        .append("statusCount", statusCount())
        .append("tasks", List.copyOf(tasksView));
  }

  public Map<BaseTask.TaskStatus, Integer> statusCount() {
    Map<BaseTask.TaskStatus, Integer> statusCount = new HashMap<>(tasksView.size());
    tasksView.forEach(task -> statusCount.merge(task.status(), 1, Math::addExact));
    return statusCount;
  }

  public String taskClassName() {
    if (tasksView.isEmpty()) {
      return "<TaskListEmpty>";
    }
    return ClassUtils.classSimpleName(tasksView.getFirst().getClass());
  }
}
