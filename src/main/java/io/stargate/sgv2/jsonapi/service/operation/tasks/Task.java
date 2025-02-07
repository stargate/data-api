package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.Optional;
import java.util.UUID;

/**
 * Interface to describe a single task that is part of an Operation.
 *
 * <p>Operations work on a group of tasks in a {@link TaskGroup}.
 *
 * <p>We have an interface to make it easier for other code to be generic when using a task, but the
 * {@link BaseTask} has the logic we need to retry and manage state etc.
 *
 * @param <SchemaT>
 */
public interface Task<SchemaT extends SchemaObject>
    extends Comparable<Task<SchemaT>>, WithWarnings.WarningsSink {

  enum TaskStatus {
    /** Initial state, the task is not configured and will not run. */
    UNINITIALIZED(false),
    /** The task is ready to run, but has not started yet. */
    READY(false),
    /** The task is in progress, it has started but has not completed yet. */
    IN_PROGRESS(false),
    /** The task has completed successfully. */
    COMPLETED(true),
    /** The task has failed, and has an error */
    ERROR(true),
    /** The task was skipped, it was not run */
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
   * Executes the task TODO: more comments
   *
   * @param commandContext
   * @return
   */
  <SubT extends Task<SchemaT>> Uni<SubT> execute(CommandContext<SchemaT> commandContext);

  /**
   * The zero based position of the task in the group.
   *
   * @return integer position
   */
  int position();

  /**
   * The unique id of the task.
   *
   * @return UUID
   */
  UUID taskId();

  /**
   * The status of the task.
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
   * Updates the Task with an error that occurred while trying to process the attempt, and sets the
   * status to {@link TaskStatus#ERROR} if no non-null failure is added.
   *
   * <p>If this method is called multiple times then only the first error is must be kept.
   *
   * <p>The {@link #execute(CommandContext)} method should only add a failure after all retries have
   * been attempted. The failure may be passed back in the request response, so returning
   * intermediate errors is not useful.
   *
   * <p>OK to add a failure to the task before calling execute, we do this for shredding errors,
   * because the attempt will not execute if there was an error. This is also why this is on the
   * interface: the task may add a failure during processing, or other classes may add one before
   * calling execute.
   *
   * @param throwable An error that happened when trying to process the attempt, ok to pass <code>
   *     null</code> it will be ignored. If a non-null failure has already been added this call will
   *     be ignored.
   * @return This task
   */
  Task<SchemaT> maybeAddFailure(Throwable throwable);

  /**
   * The <b>first</b> error that happened when trying to process the task.
   *
   * <p>Note that if an error is added to the task before execute() is called, then execute will not
   * run.
   */
  Optional<Throwable> failure();

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
