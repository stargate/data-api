package io.stargate.sgv2.jsonapi.service.operation;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public interface Task<SchemaT extends SchemaObject>
    extends Comparable<Task<SchemaT>>, WithWarnings.WarningsSink {

  enum TaskStatus {
    /** Initial state, the operation is not configured and will not run. */
    UNINITIALIZED,
    /** The operation is ready to run, but has not started yet. */
    READY,
    /*
     * The operation is in progress, it has started but has not completed yet.
     */
    IN_PROGRESS,
    /** The operation has completed successfully. */
    COMPLETED,
    /** The operation has failed, and has an error */
    ERROR,
    /** The operation was skipped, it was not run */
    SKIPPED;

    /**
     * Check if the status is terminal, meaning the operation is done and will not change state.
     *
     * @return <code>True</code> if the operation reached a terminal state
     */
    public final boolean isTerminal() {
      return this == COMPLETED || this == ERROR || this == SKIPPED;
    }
  }


  <SubT extends Task<SchemaT>> Uni<SubT> execute(CommandContext<SchemaT> commandContext);

  /**
   * The zero based position of the document or row in the request from the user.
   *
   * @return integer position
   */
  int position();

  UUID taskId();

  TaskStatus status();

  /**
   * Set the status to {@link TaskStatus#SKIPPED} if the status is {@link
   * TaskStatus#READY}. If not {@link TaskStatus#READY} then throws an {@link
   * IllegalStateException}.
   *
   * <p>This is a small method, but is here and public to make it clear what the intention is for
   * external callers.
   *
   * @return this object, cast to {@link SubT} for chaining methods.
   */
  <SubT extends Task<SchemaT>> SubT setSkippedIfReady();

  /**
   * The <b>last</b> error that happened when trying to process the attempt.
   *
   * <p>Note that is an error is added to the attempt before execute() is called, then execute will
   * not run and so you could say it is the first error in the case. But more logically, we do not
   * execute if we have an error.
   *
   * <p>This is only updated after any retries have completed, the throwable is passed through the
   * {@link DriverExceptionHandler} provided in the {@link #execute(CommandQueryExecutor,
   * DefaultDriverExceptionHandler.Factory)} method.
   */
  Optional<Throwable> failure();

  /**
   * The warnings are filtered using suppressed warning and only those to be added to the response.
   *
   * <p>See {@link OperationAttemptPage} for how warnings are included in the response.
   *
   * @return An unmodifiable list of warnings, never <code>null</code>
   */
  List<WarningException> warningsExcludingSuppressed() ;

  @Override
  default int compareTo(Task<SchemaT> other) {
    return Integer.compare(position(), other.position());
  }
}
