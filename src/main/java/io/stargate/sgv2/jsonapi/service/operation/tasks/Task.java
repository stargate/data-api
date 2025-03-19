package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Optional;
import java.util.UUID;

/**
 * Interface to describe a single task that is part of an Operation.
 *
 * <p>Operations work on a group of tasks in a {@link TaskGroup}.
 *
 * <p>We have an interface to make it easier for other code to be generic when using a task, the
 * {@link BaseTask} has the logic we need to retry and manage state etc. so subclass that.
 *
 * @param <SchemaT> The type of the schema object that the task works on
 */
public interface Task<SchemaT extends SchemaObject>
    extends Comparable<Task<SchemaT>>, WithWarnings.WarningsSink, Recordable {

  enum TaskStatus {
    /** Initial state, the task is not configured and will not run. */
    UNINITIALIZED(false),
    /** The task is ready to run, but has not started yet. */
    READY(false),
    /** The task is in progress, it has started but has not completed yet. */
    IN_PROGRESS(false),
    /** The task has completed successfully, this is a terminal state. */
    COMPLETED(true),
    /** The task has failed, and has an error, this is a terminal state */
    ERROR(true),
    /** The task was skipped, it was not run, this is a terminal state */
    SKIPPED(true);

    private final boolean isTerminal;

    TaskStatus(boolean isTerminal) {
      this.isTerminal = isTerminal;
    }

    /**
     * Check if the status is terminal, meaning the task has completed and will not change state.
     *
     * @return <code>True</code> if the task reached a terminal state
     */
    public final boolean isTerminal() {
      return isTerminal;
    }
  }

  /**
   * Execute the task, using the resources on the supplied {@link CommandContext}.
   *
   * <p>The tasks should start executing if and only if the status is {@link TaskStatus#READY}.
   *
   * <p>Tasks do not return the result of their execution, they should retain this as state, and
   * make it available on their public interface. The result of running the task(s) is then
   * aggregated together by a {@link TaskAccumulator} to get the final result of running all the
   * tasks.
   *
   * <p>Tasks may throw an exception from this method, if so it will be caught and first passed
   * through the {@link TaskRetryPolicy} to determine if this method should be retried. The last
   * exception thrown from the method will be passed through a {@link
   * io.stargate.sgv2.jsonapi.exception.ExceptionHandler} to where it can be re-mapped into
   * something we want to return to a user, and then set as the failure via {@link
   * #maybeAddFailure(Throwable)} so the accumulator can see it.
   *
   * <p>
   *
   * @param commandContext The context for the task that contains any resources or configuration
   *     needed.
   * @return The task as a subclass, so that the task can be accumulated into a {@link
   *     TaskAccumulator}.
   * @param <SubT> The type of the class this is implementing {@link Task}
   */
  <SubT extends Task<SchemaT>> Uni<SubT> execute(CommandContext<SchemaT> commandContext);

  /**
   * The zero based position of the task in the group, the value must never change.
   *
   * @return integer position
   */
  int position();

  /**
   * The unique id of the task, the value must never change.
   *
   * @return UUID that is unique to this task
   */
  UUID taskId();

  /**
   * The status of the task, the status should change while the task is processing until it reaches
   * a terminal state.
   *
   * @return {@link TaskStatus} status, the status may change but never after it has reached a
   *     {@link TaskStatus#isTerminal()} state.
   */
  TaskStatus status();

  /**
   * Set the status to {@link TaskStatus#SKIPPED} if the status is {@link TaskStatus#READY}.
   *
   * <p>If not in the {@link TaskStatus#READY} the task must add a {@link IllegalStateException} via
   * {@link #maybeAddFailure(Throwable)}.
   *
   * <p>This is a small method, but is here and public to make it clear what the intention is for
   * external callers. We use this when skipping tasks in a sequential task group.
   *
   * @return This task
   */
  Task<SchemaT> setSkippedIfReady();

  /**
   * Updates the Task with an error that occurred while trying to process the user request, and sets
   * the status to {@link TaskStatus#ERROR} if no non-null failure is added.
   *
   * <p>If this method is called multiple times then only the first error is must be kept.
   *
   * <p>This method is not normmaly called
   *
   * <p>OK to add a failure to the task before calling execute, we do this for shredding errors,
   * because the attempt will not execute if there was an error. This is also why this is on the
   * interface: the task may add a failure during processing, or other classes may add one before
   * calling execute.
   *
   * @param runtimeException An error that happened when trying to process the attempt, ok to pass
   *     <code>
   *                         null</code> it will be ignored. If a non-null failure has already been
   *     added this call will be ignored.
   * @return This task
   */
  Task<SchemaT> maybeAddFailure(Throwable runtimeException);

  /**
   * The <b>first</b> error that happened when trying to process the task.
   *
   * <p>Note that if an error is added to the task before execute() is called, then execute will not
   * run.
   */
  Optional<Throwable> failure();

  /** Helper method to build a string with the position and taskId, used in logging. */
  default String taskDesc() {

    return String.format(
        "class=%s, position=%d, taskId=%s, status=%s",
        getClass().getSimpleName().isBlank() ? getClass().getName() : getClass().getSimpleName(),
        position(),
        taskId(),
        status());
  }

  /**
   * Compares tasks based on their {@link #position()}, lower values are first.
   *
   * @param other the object to be compared.
   * @return a negative integer, zero, or a positive integer as this object is less than, equal to,
   *     or greater than the specified object.
   */
  @Override
  default int compareTo(Task<SchemaT> other) {
    return Integer.compare(position(), other.position());
  }
}
