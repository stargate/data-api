package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnsDef;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.time.Duration;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single query we want to run against the database.
 *
 * <p>This can be any type of DML or DDL statement, the operation is responsible for executing the
 * query and handling any errors and retries. Used together with the {@link
 * OperationAttemptContainer}, {@link OperationAttemptPageBuilder} and {@link GenericOperation}
 *
 * <p>Sub classes are responsbile for sorting out what query to run, and how to handle the results.
 * This superclass knows how to run a generic query with config such as retries.
 *
 * <p>The class has a basic state model for tracking the status of the operation. <b>NOTE</b>
 * subclasses much set the state of {@link OperationStatus#READY} using {@link
 * #setStatus(OperationStatus)}, other transitions are handled by the superclass.All handling of the
 * state must be done through the methods, do not access the state directly. <b>NOTE:</b> This class
 * is not thread safe, it is used in the Smallry processing and is not expected to be used in a
 * multi-threaded environment.
 *
 * @param <SubT> Subtype of the OperationAttempt, used for chaining methods.
 * @param <SchemaT> The type of the schema object that the operation is working with.
 */
public abstract class OperationAttempt<
        SubT extends OperationAttempt<SubT, SchemaT>, SchemaT extends SchemaObject>
    implements Comparable<SubT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OperationAttempt.class);

  public enum OperationStatus {
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

  private final int position;
  protected final SchemaT schemaObject;
  protected final RetryPolicy retryPolicy;

  protected final UUID attemptId = UUID.randomUUID();

  private final List<APIException> warnings = new ArrayList<>();
  private Throwable failure;

  // Keep this private, so sub-classes set through setter incase we need to syncronize later
  private OperationStatus status = OperationStatus.UNINITIALIZED;

  /**
   * Create a new {@link OperationAttempt} with the provided position, schema object and retry
   * policy.
   *
   * @param position The 0 based position of the attempt in the container of attempts. Attempts are
   *     ordered by position, for sequential processing and for rebuilding the response in the
   *     correct order (e.g. for inserting many documents)
   * @param schemaObject The schema object that the operation is working with.
   * @param retryPolicy The {@link RetryPolicy} to use when running the operation, if there is no
   *     retry policy then use {@link RetryPolicy#NO_RETRY}
   */
  protected OperationAttempt(int position, SchemaT schemaObject, RetryPolicy retryPolicy) {
    this.position = position;
    this.schemaObject = Objects.requireNonNull(schemaObject, "schemaObject cannot be null");
    this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy cannot be null");
  }

  /**
   * Utility to downcast this object to the subtype, useful for chaining methods.
   *
   * @return this case to {@link SubT}
   */
  @SuppressWarnings("unchecked")
  protected SubT downcast() {
    return (SubT) this;
  }

  /**
   * Executes this attempt, delegating down to the subclass to build the query.
   *
   * @param queryExecutor The {@link CommandQueryExecutor} to use for executing the query, this
   *     handles interacting with the driver.
   * @param exceptionHandler The handler to use for exceptions thrown by the driver, exceptions
   *     thrown by the driver are passed through here before being added to the {@link
   *     OperationAttempt}.
   * @return A {@link Uni} of this object, cast to the {@link SubT} for chaining methods. This
   *     object's state will be updated as the operation runs.
   */
  public Uni<SubT> execute(
      CommandQueryExecutor queryExecutor, DriverExceptionHandler<SchemaT> exceptionHandler) {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "execute() - starting subclass={}, status={}, {}",
          getClass().getSimpleName(),
          status(),
          positionAndAttemptId());
    }

    if (!swapStatus("start execute()", OperationStatus.READY, OperationStatus.IN_PROGRESS)) {
      return Uni.createFrom().item(downcast());
    }

    return Uni.createFrom()
        .item(() -> executeIfInProgress(queryExecutor)) // Wraps any error from execute() into a Uni
        .flatMap(uni -> uni) // Unwrap Uni<Uni<AsyncResultSet>> to Uni<AsyncResultSet>
        .onFailure(this::decideRetry)
        .retry()
        .withBackOff(retryPolicy.delay(), retryPolicy.delay())
        .atMost(retryPolicy.maxRetries())
        .onItemOrFailure()
        .transform((resultSet, throwable) -> onCompletion(exceptionHandler, resultSet, throwable))
        .invoke(
            () -> {
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "execute() - finished subclass={}, {}",
                    getClass().getSimpleName(),
                    positionAndAttemptId());
              }
            });
  }

  protected Uni<AsyncResultSet> executeIfInProgress(CommandQueryExecutor queryExecutor) {
    // First things first: did we already fail? If so we do not execute, we can just return self.
    // In practice this should not happen, because the execute() ensures the state is READY before
    // starting it is
    // here incase we hae missed something in the retry
    if (status() == OperationStatus.ERROR) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "executeIfInProgress() - state was {}, will not execute {}",
            status(),
            positionAndAttemptId());
      }
      return Uni.createFrom().item(null);
    }

    // same as above for this checkProgress , we should only be IN_PROGRESS is anything else
    // checkProgress will
    // set a failure and return false.
    return checkStatus("executeIfInProgress", OperationStatus.IN_PROGRESS)
        ? executeStatement(queryExecutor)
        : Uni.createFrom().item(null);
  }

  /**
   * Sublasses must implement this method to build the query and execute it, they should not do
   * anything with Uni for retry etc, that is handled in the base class {@link
   * #execute(CommandQueryExecutor, DriverExceptionHandler)}.
   *
   * @param queryExecutor the {@link CommandQueryExecutor} , subclasses should call the appropriate
   *     execute method
   * @return A {@link Uni} of the {@link AsyncResultSet} for processing the query.
   */
  protected abstract Uni<AsyncResultSet> executeStatement(CommandQueryExecutor queryExecutor);

  /**
   * Decides if the operation should be retried, using the {@link #retryPolicy} and the {@link
   * DriverExceptionHandler}.
   *
   * @param throwable The exception thrown by the driver, this has not been passed through a {@link
   *     DriverExceptionHandler}.
   * @return <code>True</code> if the operation should be retried, <code>False</code> otherwise.
   */
  protected boolean decideRetry(Throwable throwable) {

    // we should only be called when IN_PROGRESS, anything else is an invalid state e.g. if we were
    // in ERROR it means
    // we tracked an error, and then kept retrying.
    // because this function can only return boolean, we throw
    if (!checkStatus("decideRetry", OperationStatus.IN_PROGRESS)) {
      throw new IllegalStateException(
          String.format(
              "decideRetry() called when not IN_PROGRESS status=%s, %s",
              status(), positionAndAttemptId()));
    }

    var shouldRetry = retryPolicy.shouldRetry(throwable);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "decideRetry() - shouldRetry={}, {}, for throwable {}",
          shouldRetry,
          positionAndAttemptId(),
          throwable.toString());
    }

    return shouldRetry;
  }

  /**
   * Called when the operation has completed, subclasses should not normally override this method.
   * If you want to do something like capture the result set on success (non error) then override
   * {@link #onSuccess(AsyncResultSet)}.
   *
   * @param exceptionHandler The handler to use for exceptions thrown by the driver, exceptions
   *     thrown by the driver are passed through here before being added to the {@link
   *     OperationAttempt}.
   * @param resultSet The result set from the driver, this is the result of the query. May be null
   *     on error
   * @param throwable The exception thrown by the driver, this has not been passed through a {@link
   *     DriverExceptionHandler}.
   * @return this object, cast to {@link SubT} for chaining methods.
   */
  protected SubT onCompletion(
      DriverExceptionHandler<SchemaT> exceptionHandler,
      AsyncResultSet resultSet,
      Throwable throwable) {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "onCompletion() - resultSetIsNull={}, throwable={}, {}",
          resultSet == null,
          Objects.toString(throwable, "NULL"),
          positionAndAttemptId());
    }

    // sanity check, if we do not have a result set then we should have an exception
    if (resultSet == null && throwable == null) {
      throw new IllegalStateException(
          String.format(
              "onCompletion() - resultSet and throwable are both null, %s",
              positionAndAttemptId()));
    }

    var handledException =
        throwable instanceof RuntimeException
            ? exceptionHandler.maybeHandle(schemaObject, (RuntimeException) throwable)
            : throwable;

    if (LOGGER.isDebugEnabled()) {
      if (handledException != throwable) {
        LOGGER.debug(
            "onCompletion() - handledException={}, {}", handledException, positionAndAttemptId());
      }
    }

    return switch (status()) {
      case IN_PROGRESS ->
          handledException == null ? onSuccess(resultSet) : maybeAddFailure(handledException);
      case ERROR -> downcast();
      default ->
          throw new IllegalStateException(
              String.format(
                  "onCompletion() unsupported status=%s, %s", status(), positionAndAttemptId()));
    };
  }

  /**
   * Called when the operation has completed successfully, subclasses should override this method to
   * capture the result set and do any processing of the result set. Subclasses should call the base
   * to ensure the status of the attempt is updated, or set the status to {@link
   * OperationStatus#COMPLETED} themselves.
   *
   * @param resultSet The result set from the driver, this is the result of the query.
   * @return this object, cast to {@link SubT} for chaining methods.
   */
  protected SubT onSuccess(AsyncResultSet resultSet) {
    return setStatus(OperationStatus.COMPLETED);
  }

  /**
   * The zero based position of the document or row in the request from the user.
   *
   * @return integer position
   */
  public int position() {
    return position;
  }

  public UUID attemptId() {
    return attemptId;
  }

  public OperationStatus status() {
    return status;
  }

  protected SubT setStatus(OperationStatus newStatus) {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "setStatus() - status changing from {} to {} for {}",
          status(),
          newStatus,
          positionAndAttemptId());
    }
    status = newStatus;
    return downcast();
  }

  /**
   * Check the status of the attempt, if it is the expected status then return this object,
   * otherwise throw an {@link IllegalStateException}.
   *
   * @param context short descriptive text about what is being checked, used in the exception
   * @param expectedStatus The status that is expected
   * @return True if the attempt is in the expected state, otherwise a {@link IllegalStateException}
   *     is added to the attempt, ERROR state set, and false is returned.
   */
  protected boolean checkStatus(String context, OperationStatus expectedStatus) {

    if (status().equals(expectedStatus)) {
      return true;
    }

    maybeAddFailure(
        new IllegalStateException(
            String.format(
                "OperationAttempt: checkStatus() failed for %s, expected %s but actual %s for %s",
                context, expectedStatus, status(), positionAndAttemptId())));
    return false;
  }

  /**
   * Check the status of the attempt is a terminal status, if not throw an {@link
   * IllegalStateException}.
   *
   * <p>useful for subclasses to confirm the operation is done.
   *
   * @param context short descriptive text about what is being checked, used in the exception
   * @return this object, cast to {@link SubT} for chaining methods.
   */
  public SubT checkTerminal(String context) {

    if (status().isTerminal()) {
      return downcast();
    }

    throw new IllegalStateException(
        String.format(
            "OperationAttempt: checkTerminal() failed for %s, non-terminal status %s for %s",
            context, status(), positionAndAttemptId()));
  }

  /**
   * Swap the status of the attempt
   *
   * @return True is the status was swapped, false otherwise
   */
  protected boolean swapStatus(
      String context, OperationStatus expectedStatus, OperationStatus newStatus) {

    if (checkStatus(context, expectedStatus)) {
      setStatus(newStatus);
      return true;
    }
    return false;
  }

  /**
   * Set the status to {@link OperationStatus#SKIPPED} if the status is {@link
   * OperationStatus#READY}. If not {@link OperationStatus#READY} then throws an {@link
   * IllegalStateException}.
   *
   * <p>This is a small method, but is here and public to make it clear what the intention is for
   * external callers.
   *
   * @return this object, cast to {@link SubT} for chaining methods.
   */
  public SubT setSkippedIfReady() {
    swapStatus("setSkippedIfReady()", OperationStatus.READY, OperationStatus.SKIPPED);
    return downcast();
  }

  /**
   * The <b>last</b> error that happened when trying to process the attempt.
   *
   * <p>Note that is an error is added to the attempt before execute() is called, then execute will
   * not run and so you could say it is the first error in the case. But more logically, we do not
   * execute if we have an error.
   *
   * <p>This is only updated after any retries have completed, the throwable is passed through the
   * {@link DriverExceptionHandler} provided in the {@link #execute(CommandQueryExecutor,
   * DriverExceptionHandler)} method.
   */
  public Optional<Throwable> failure() {
    return Optional.ofNullable(failure);
  }

  /**
   * Updates the attempt with an error that occurred while trying to process the attempt, updates
   * the attempt status to {@link OperationStatus#ERROR} if no non-null failure is added.
   *
   * <p>If this method is called multiple times then only the first error is kept. The {@link
   * #execute(CommandQueryExecutor, DriverExceptionHandler)} only calls this after all retries have
   * been attempted.
   *
   * <p>OK to add a failure to the attempt before calling execute, we do this for shredding errors,
   * because the attempt will not execute if there was an error.
   *
   * @param throwable An error that happened when trying to process the attempt, ok to pass <code>
   *     null
   *     </code> it will be ignored. If a non-null failure has already been added this call will be
   *     ignored.
   * @return This object, cast to {@link SubT} for chaining methods.
   */
  public SubT maybeAddFailure(Throwable throwable) {

    if (failure == null) {
      failure = throwable;
      if (failure != null) {
        if (LOGGER.isDebugEnabled()) {
          // deliberately calling toString() here, because we want to log the exception stack,
          // exception are not always bad
          LOGGER.debug(
              "maybeAddFailure() - added failure for {}, failure={}",
              positionAndAttemptId(),
              failure.toString());
        }
        setStatus(OperationStatus.ERROR);
      }
    } else if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "maybeAddFailure() - will not add failure for {}, because has existing failure={}, attempted new failure={}",
          positionAndAttemptId(),
          failure.toString(),
          throwable.toString());
    }
    return downcast();
  }

  /**
   * Adds a warning to the attempt, warnings are not errors, but are messages that can be included
   * in the response.
   *
   * <p>See {@link OperationAttemptPage} for how warnings are included in the response.
   *
   * @param warning The warning message to add, cannot be <code>null</code> or blank.
   */
  public void addWarning(APIException warning) {
    warnings.add(Objects.requireNonNull(warning, "warning cannot be null"));
  }

  /**
   * The warnings that have been added to the attempt, these are messages that can be included in
   * the response.
   *
   * <p>See {@link OperationAttemptPage} for how warnings are included in the response.
   *
   * @return An unmodifiable list of warnings, never <code>null</code>
   */
  public List<APIException> warnings() {
    return List.copyOf(warnings);
  }

  /**
   * Called to get the description of the schema to use when building the response.
   *
   * @return The optional object that describes the schema, if present the object will be serialised
   *     to JSON and included in the response status such as {@link
   *     CommandStatus#PRIMARY_KEY_SCHEMA}. How this is included in the response is up to the {@link
   *     OperationAttemptPage} that is building the response.
   */
  public Optional<ColumnsDef> schemaDescription() {
    return Optional.empty();
  }

  /** helper method to build a string with the position and attemptId, used in logging. */
  public String positionAndAttemptId() {
    return String.format("position=%d, attemptId=%s", position, attemptId);
  }

  @Override
  public String toString() {
    return new StringBuilder("OperationAttempt{")
        .append("subtype={")
        .append(getClass().getSimpleName())
        .append("}, ")
        .append("position=")
        .append(position)
        .append(", ")
        .append("status=")
        .append(status)
        .append(", ")
        .append("attemptId=")
        .append(attemptId)
        .append(", ")
        .append("failure=")
        .append(failure)
        .append("}")
        .toString();
  }

  /**
   * Compares the position of this attempt to another.
   *
   * @param other the object to be compared.
   * @return Result of {@link Integer#compare(int, int)}
   */
  @Override
  public int compareTo(SubT other) {
    return Integer.compare(position(), other.position());
  }

  /**
   * A policy for retrying an attempt, if the attempt does not want to retry then it should use
   * {@link RetryPolicy#NO_RETRY}.
   *
   * <p>To implement a custom retry policy, subclass this class and override {@link
   * #shouldRetry(Throwable)}.
   */
  public static class RetryPolicy {

    public static final RetryPolicy NO_RETRY = new RetryPolicy();

    private final int maxRetries;
    private final Duration delay;

    private RetryPolicy() {
      this(1, Duration.ofMillis(1));
    }

    /**
     * Construct a new retry policy with the provided max retries and delay.
     *
     * @param maxRetries the number of retries after the initial attempt, must be >= 1
     * @param delay the delay between retries, must not be <code>null</code>
     */
    public RetryPolicy(int maxRetries, Duration delay) {
      // This is a requirement of UniRetryAtMost that is it >= 1, however UniRetry.atMost() says it
      // must be >= 0
      if (maxRetries < 1) {
        throw new IllegalArgumentException("maxRetries must be >= 1");
      }
      this.maxRetries = maxRetries;
      this.delay = Objects.requireNonNull(delay, "delay cannot be null");
    }

    public int maxRetries() {
      return maxRetries;
    }

    public Duration delay() {
      return delay;
    }

    public boolean shouldRetry(Throwable throwable) {
      return false;
    }
  }
}
