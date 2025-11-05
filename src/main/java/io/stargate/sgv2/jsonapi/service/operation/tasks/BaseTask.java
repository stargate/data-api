package io.stargate.sgv2.jsonapi.service.operation.tasks;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of a {@link @Task} that manages the state transitions, retries etc.
 *
 * <p>This is stilla generic task, i.e. it may be working with the database, or an embedding
 * provider or any other type of task.
 *
 * <p>The class has a state transition model for tracking the status of the operation.
 *
 * <p><b>NOTE</b> subclasses must set the state to {@link TaskStatus#READY} using {@link
 * #setStatus(TaskStatus)}, other transitions are handled by the superclass. All handling of the
 * state must be done through the methods, do not access the state directly.
 *
 * <p><b>NOTE:</b> This class is not thread safe, it is used in the Smallrye processing and is not
 * expected to be used in a multithreaded environment.
 *
 * @param <SchemaT> The type of the schema object that the task is working with.
 * @param <ResultSupplierT> The type of the result supplier that will be provided by the subclass
 *     via {@link #buildResultSupplier(CommandContext)}.
 * @param <ResultT> The type of the result that the task will return, via the {@link
 *     ResultSupplierT}
 */
public abstract class BaseTask<
        SchemaT extends SchemaObject,
        ResultSupplierT extends BaseTask.UniSupplier<ResultT>,
        ResultT>
    implements Task<SchemaT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseTask.class);

  // Keep this private, so subclasses set through setter incase we need to synchronize later
  // use {@link #setStatus(TaskStatus)} to change the status.
  private TaskStatus status = TaskStatus.UNINITIALIZED;

  private final int position;
  private final UUID taskId = UUID.randomUUID();

  protected final SchemaT schemaObject;
  protected final TaskRetryPolicy retryPolicy;

  private Throwable failure;
  private final List<WarningException> warnings = new ArrayList<>();
  private final List<WarningException.Code> suppressedWarnings = new ArrayList<>();

  /** The current result supplier, supplied by the subclass every time we try / re-try */
  private ResultSupplierT resultSupplier = null;

  /** The command context for the current execution, set when execute() is called */
  protected CommandContext<SchemaT> commandContext = null;

  // Number of times the task has been retried, we started with 0 and only increase when we
  // decide to retry.
  private int retryCount = 0;

  /**
   * Create a new {@link BaseTask} with the provided position, schema object and retry policy.
   *
   * @param position The 0 based position of the task in the container of tasks. Tasks are ordered
   *     by position, for sequential processing and for rebuilding the response in the correct order
   *     (e.g. for inserting many documents)
   * @param schemaObject The schema object that the task is working with.
   * @param retryPolicy The {@link TaskRetryPolicy} to use when running the task, if there is no
   *     retry policy then use {@link TaskRetryPolicy#NO_RETRY}
   */
  protected BaseTask(int position, SchemaT schemaObject, TaskRetryPolicy retryPolicy) {
    if (position < 0) {
      throw new IllegalArgumentException("position must be >= 0");
    }
    this.position = position;
    this.schemaObject = Objects.requireNonNull(schemaObject, "schemaObject cannot be null");
    this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy cannot be null");
  }

  // =================================================================================================
  // Task interface implementation
  // =================================================================================================

  /** {@inheritDoc} */
  @Override
  public <SubT extends Task<SchemaT>> Uni<SubT> execute(CommandContext<SchemaT> commandContext) {

    // Save the command context for use in exception handling
    this.commandContext = commandContext;

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("execute() - starting {}", taskDesc());
    }

    if (!swapStatus("start execute()", TaskStatus.READY, TaskStatus.IN_PROGRESS)) {
      return Uni.createFrom().item(this::downcast);
    }

    long startNano = System.nanoTime();
    return Uni.createFrom()
        .item(
            () -> executeIfInProgress(commandContext)) // Wraps any error from execute() into a Uni
        .flatMap(uni -> uni) // Unwrap Uni<Uni<AsyncResultSet>> to Uni<AsyncResultSet>
        .onFailure(this::decideRetry)
        .retry()
        .withBackOff(retryPolicy.delay(), retryPolicy.delay())
        .atMost(retryPolicy.maxRetries())
        .onItemOrFailure()
        .transform(
            (result, throwable) -> {
              onCompletion(result, throwable);
              return (SubT) this; // TODO - downcast() does not work here
            })
        .invoke(
            () -> {
              if (LOGGER.isDebugEnabled()) {
                double durationMs = (System.nanoTime() - startNano) / 1_000_000.0;
                LOGGER.debug(
                    "execute() - finished {}, durationMs={}, subclass={}",
                    taskDesc(),
                    durationMs,
                    getClass().getSimpleName());
              }
            });
  }

  /** {@inheritDoc} */
  @Override
  public int position() {
    return position;
  }

  /** {@inheritDoc} */
  @Override
  public UUID taskId() {
    return taskId;
  }

  /** {@inheritDoc} */
  @Override
  public TaskStatus status() {
    return status;
  }

  /** {@inheritDoc} */
  @Override
  public Task<SchemaT> setSkippedIfReady() {
    swapStatus("setSkippedIfReady()", Task.TaskStatus.READY, Task.TaskStatus.SKIPPED);
    return upcastToTask();
  }

  /** {@inheritDoc} */
  @Override
  public Task<SchemaT> maybeAddFailure(Throwable throwable) {

    if (failure == null) {
      failure = throwable;
      if (failure != null) {
        if (LOGGER.isDebugEnabled()) {
          // deliberately calling toString() here, because we do not want to log the exception
          // stack,
          // exception are not always bad (if we pass the exception it will log the stack)
          LOGGER.debug(
              "maybeAddFailure() - added failure for {}, failure={}",
              taskDesc(),
              Objects.toString(failure));
        }
        setStatus(TaskStatus.ERROR);
      }
    } else if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "maybeAddFailure() - will not add failure for {}, because has existing failure={}, attempted new failure={}",
          taskDesc(),
          Objects.toString(failure),
          Objects.toString(throwable));
    }
    return upcastToTask();
  }

  /** {@inheritDoc} */
  @Override
  public Optional<Throwable> failure() {
    return Optional.ofNullable(failure);
  }

  // =================================================================================================
  // Subclass API
  // =================================================================================================

  /**
   * Function describing the function a subclass must provide to get the results of the task.
   *
   * <p>This is just a function that looks like for example:
   *
   * <pre>
   *   Uni<AsyncResultSet> get();
   * </pre>
   *
   * @param <ResulT> The type of the result that the task will return, e.g. an {@link
   *     AsyncResultSet}.
   */
  @FunctionalInterface
  public interface UniSupplier<ResulT> extends Supplier<Uni<ResulT>> {}

  /**
   * Subclasses must implement this method to provide a supplier that will fetch the results (e.g.
   * from db) and return them when the supplier is called. The results must not be fetched until the
   * supplier is called to avoid blocking the uni.
   *
   * <p>We use a generic type for the result supplier so that subclasses can attach additional
   * execution state that can later be used in the retry or exception handling. For example {@link
   * DBTask} can include the statement so it can be used in error messages.
   *
   * <p>The supplied should not do anything with Uni for retry etc., that is handled in {@link
   * #execute(CommandContext)} using the {@link TaskRetryPolicy}.
   *
   * @param commandContext The {@link CommandContext} for the task.
   * @return A {@link ResultSupplierT} that when called will fetch and return the {@link ResultT}.
   */
  protected abstract ResultSupplierT buildResultSupplier(CommandContext<SchemaT> commandContext);

  /**
   * Subclasses must implement to handle an error they generated and potentially map it into an
   * error they want to return to the user such as via a {@link
   * io.stargate.sgv2.jsonapi.exception.ExceptionHandler}.
   *
   * <p>Called after we have completed processing a task, on the first error passed to {@link
   * Task#maybeAddFailure(RuntimeException)}. Note that any errors that resulted in a retry will not
   * be tracked, only the final error.
   *
   * @param resultSupplier The {@link ResultSupplierT} returned by the subclass from {@link
   *     #buildResultSupplier(CommandContext)}. Note: this may be <b>null</b> if the task either did
   *     not start executing or did not get to build a valid statement, e.g. a request error was
   *     found during the creation of the task such as invalid data type.
   * @param runtimeException The exception to handle.
   * @return The exception to return to the user, if null then the error is swallowed. Generally we
   *     want to turn the exception into a {@link io.stargate.sgv2.jsonapi.exception.APIException}.
   */
  protected abstract RuntimeException maybeHandleException(
      ResultSupplierT resultSupplier, RuntimeException runtimeException);

  /**
   * Called when the operation has completed successfully, subclasses should override this method to
   * capture the result e.g. so it can be obtained when building a results page later.
   *
   * <p>Subclasses <b>should</b> call the base to ensure the status of the task is updated, or set
   * the status to {@link TaskStatus#COMPLETED} themselves.
   *
   * <p>NOTE: this has void return, it used to try to return a generic subtype, but it was too
   * fragile.
   *
   * @param result The {@link ResultT} generated by the task, e.g. an {@link AsyncResultSet}.
   */
  protected void onSuccess(ResultT result) {
    setStatus(TaskStatus.COMPLETED);
  }

  // =================================================================================================
  // Internal Implementation - generally not intended to be overridden
  // =================================================================================================

  /**
   * Utility upcast <code>this</code> to the {@link Task} interface for use when implementing
   * methods from that interface that need to return on the interface.
   *
   * @return this object, cast to {@link Task}
   */
  protected Task<SchemaT> upcastToTask() {
    return this;
  }

  @SuppressWarnings("unchecked")
  protected <SubT extends Task<SchemaT>> SubT downcast() {
    return (SubT) this;
  }

  /**
   * Executes this task, delegating down to the subclass to get the {@link ResultSupplierT}.
   *
   * @param commandContext The {@link CommandContext} for the task.
   * @return A {@link Uni} the {@link ResultT} so the subclass can inspect it.
   */
  protected Uni<ResultT> executeIfInProgress(CommandContext<SchemaT> commandContext) {

    // First things first: did we already fail? If so we do not execute, we can just return self.
    // In practice this should not happen, because the execute() ensures the state is READY before
    // starting it is here incase we hae missed something in the retry
    if (status() == TaskStatus.ERROR) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("executeIfInProgress() - in ERROR state, will not execute {}", taskDesc());
      }
      return Uni.createFrom().item(null);
    }

    // same as above for this check, we should only be IN_PROGRESS if anything else
    // checkStatus will set a failure and return false.
    if (!checkStatus("executeIfInProgress", TaskStatus.IN_PROGRESS)) {
      return Uni.createFrom().item(null);
    }

    resultSupplier = buildResultSupplier(commandContext);
    if (resultSupplier == null) {
      throw new IllegalStateException(
          "executeIfInProgress() - buildStatementExecutor() returned null, " + taskDesc());
    }

    return resultSupplier.get();
  }

  /**
   * Decides if the operation should be retried, using the {@link #retryPolicy}
   *
   * @param throwable The exception thrown by the task when executing, the exception should not have
   *     been "handled", i.e, parsed into the exception we want to return to the user.
   * @return <code>True</code> if the operation should be retried, <code>False</code> otherwise.
   */
  protected boolean decideRetry(Throwable throwable) {

    // we should only be called when IN_PROGRESS, anything else is an invalid state e.g. if we were
    // in ERROR it means we tracked an error, and then kept retrying.
    // because this function can only return boolean, we throw
    if (!checkStatus("decideRetry", TaskStatus.IN_PROGRESS)) {
      throw new IllegalStateException(
          String.format(
              "decideRetry() called when not IN_PROGRESS status=%s, %s", status(), taskDesc()));
    }

    var shouldRetry = retryPolicy.shouldRetry(throwable);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "decideRetry() - retry decision: {}, shouldRetry={}, retryCount={}, retryPolicy.maxRetries={}, policy={}, for throwable {}",
          taskDesc(),
          shouldRetry,
          retryCount,
          retryPolicy.maxRetries(),
          retryPolicy.getClass().getName(), // full name for so lambda can be used
          Objects.toString(throwable));
    }
    retryCount++;
    return shouldRetry;
  }

  /**
   * Called when the operation has completed, could be successfully or error.
   *
   * <p>Subclasses should not normally override this method. If you want to do something like
   * capture the result set on success (non error) then override {@link #onSuccess(ResultT)}.
   *
   * <p>NOTE: this has void return, it used to try to return a generic subtype, but it was too
   * fragile.
   *
   * @param result The {@link ResultT} from the subclasses {@link ResultSupplierT}, e.g. an
   *     AsyncResultSet. May be null on error
   * @param throwable The exception thrown by the {@link ResultSupplierT}, this is the final error
   *     after any retries but before it has been mapped into the error to return to the user.
   */
  protected void onCompletion(ResultT result, Throwable throwable) {

    if (LOGGER.isDebugEnabled()) {
      // in a lot of places we only want to log the toString() of the exception, not the stack
      // we want to log the full exception here as the one place we have the stack
      LOGGER.debug(
          "onCompletion() - task completed %s, result is null=%s (throwable in message if present)"
              .formatted(taskDesc(), result == null),
          throwable);
    }

    // sanity check, if we do not have a result then we should have an exception
    if (result == null && throwable == null) {
      throw new IllegalStateException(
          String.format("onCompletion() - result and throwable are both null, %s", taskDesc()));
    }

    // handle the error, it is turned into what we want to return to the user
    // the subclass will know how to get an ErrorMapper to do this.
    var handledException =
        throwable instanceof RuntimeException re
            ? maybeHandleException(resultSupplier, re)
            : throwable;

    if ((handledException == null && throwable != null) && LOGGER.isWarnEnabled()) {
      // this means we are swallowing an error, may be correct but make sure we warn
      LOGGER.warn(
          "onCompletion() - exception handler returned null so error is swallowed {}, throwable={}",
          taskDesc(),
          Objects.toString(throwable));
    }

    if (LOGGER.isDebugEnabled() && (null != throwable)) {
      // we started with an exception, so log what it is now
      LOGGER.debug(
          "onCompletion() - after exception handling {}, handledException={}",
          taskDesc(),
          Objects.toString(handledException));
    }

    // If we were in progress, we will switch state to either COMPLETE by calling onSuccess
    // or ERROR by calling maybeAddFailure
    // the other checks here are a sanity check on the state
    switch (status()) {
      case IN_PROGRESS -> {
        // above checks make sure we have either a result or an exception
        if (result != null) {
          onSuccess(result);
        } else {
          maybeAddFailure(handledException);
        }
      }
      case ERROR -> {
        // this is ok, already in terminal state
      }
      default ->
          throw new IllegalStateException(
              String.format("onCompletion() unsupported status=%s, %s", status(), taskDesc()));
    }
    ;
  }

  /**
   * Set the status of the task, this is the only way to change the status of the task.
   *
   * <p>NOTE: this has void return, it used to try to return a generic subtype, but it was too
   * fragile.
   *
   * @param newStatus The new status to set.
   */
  protected void setStatus(TaskStatus newStatus) {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("setStatus() - status changing to {} for {}", newStatus, taskDesc());
    }
    status = newStatus;
  }

  /**
   * Check the status of the task, if it is the expected status then return this object, otherwise
   * set a {@link IllegalStateException} via {@link Task#maybeAddFailure(RuntimeException)} to
   * change the status to {@link TaskStatus#ERROR}.
   *
   * @param context short descriptive text about what is being checked, used in the exception
   * @param expectedStatus The status that is expected
   * @return True if the task is in the expected state, otherwise a {@link IllegalStateException} is
   *     added to the task via {@link Task#maybeAddFailure(RuntimeException)} and false is returned.
   */
  protected boolean checkStatus(String context, TaskStatus expectedStatus) {

    if (status().equals(expectedStatus)) {
      return true;
    }

    maybeAddFailure(
        new IllegalStateException(
            String.format(
                "BaseTask: checkStatus() - failed for %s, expected %s but actual %s for %s",
                context, expectedStatus, status(), taskDesc())));
    return false;
  }

  /**
   * Swap the status of the task, if the status is the expected status.
   *
   * @param context short descriptive text about what is being checked, used in the exception
   * @param expectedStatus The status that is expected
   * @param newStatus The status to set if the current status is the expected status.
   * @return True is the status was swapped, false otherwise. The status is checked with {@link
   *     #checkStatus(String, TaskStatus)} so if not in expected status an exception is added to the
   *     task.
   */
  protected boolean swapStatus(String context, TaskStatus expectedStatus, TaskStatus newStatus) {

    if (checkStatus(context, expectedStatus)) {
      setStatus(newStatus);
      return true;
    }
    return false;
  }

  /**
   * Check the status of the task is a terminal status, if not throw an {@link
   * IllegalStateException}.
   *
   * <p>Useful for subclasses to confirm the operation is done.
   *
   * <p>NOTE: this has void return, it used to try to return a generic subtype, but it was too
   * fragile.
   *
   * @param context short descriptive text about what is being checked, used in the exception
   */
  protected void assertTerminalStatus(String context) {

    if (status().isTerminal()) {
      return;
    }

    throw new IllegalStateException(
        String.format(
            "BaseTask: checkTerminal() failed for %s, non-terminal status %s for %s",
            context, status(), taskDesc()));
  }

  /**
   * Pretty printing to help with logging and tests to better format the details of a task, see
   * {@link PrettyPrintable#toString(PrettyPrintableRecorder)}
   */
  @Override
  public Recordable.DataRecorder recordTo(Recordable.DataRecorder dataRecorder) {
    return dataRecorder
        .append("position", position)
        .append("status", status)
        .append("taskId", taskId)
        .append("schemaObject.type", schemaObject.type())
        .append("schemaObject.name", schemaObject.name())
        .append("retryPolicy", retryPolicy)
        .append("warnings", warnings)
        .append("suppressedWarnings", suppressedWarnings)
        .append("failure", failure);
  }

  // =================================================================================================
  // WithWarnings.WarningsSink implementation
  // =================================================================================================

  /** {@inheritDoc} */
  @Override
  public void addWarning(WarningException warning) {
    warnings.add(Objects.requireNonNull(warning, "warning cannot be null"));
  }

  /** {@inheritDoc} */
  @Override
  public void addSuppressedWarning(WarningException.Code suppressedWarning) {
    suppressedWarnings.add(
        Objects.requireNonNull(suppressedWarning, "suppressedWarning cannot be null"));
  }

  /** {@inheritDoc} */
  @Override
  public List<WarningException> allWarnings() {
    return List.copyOf(warnings);
  }

  /** {@inheritDoc} */
  @Override
  public List<WarningException> warningsExcludingSuppressed() {
    if (suppressedWarnings.isEmpty()) {
      return warnings;
    }
    var suppressedWarningsToCheck =
        suppressedWarnings.stream().map(Enum::name).collect(Collectors.toSet());

    return warnings.stream()
        .filter(warn -> !suppressedWarningsToCheck.contains(warn.code))
        .toList();
  }
}
