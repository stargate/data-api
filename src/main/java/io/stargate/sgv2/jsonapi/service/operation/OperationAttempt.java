package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class OperationAttempt<
        SubT extends OperationAttempt<SubT, SchemaT>, SchemaT extends SchemaObject>
    implements Comparable<SubT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OperationAttempt.class);

  public enum OperationStatus {
    UNINITIALIZED,
    READY,
    IN_PROGRESS,
    COMPLETED,
    ERROR,
    SKIPPED;

    public final boolean isTerminal() {
      return this == COMPLETED || this == ERROR || this == SKIPPED;
    }
  }

  private final int position;
  protected final SchemaT schemaObject;
  protected final RetryPolicy retryPolicy;

  protected final UUID attemptId = UUID.randomUUID();
  private Throwable failure;

  // Keep this private, so sub-classes set through setter incase we need to syncronize later
  private OperationStatus status = OperationStatus.UNINITIALIZED;

  protected OperationAttempt(int position, SchemaT schemaObject, RetryPolicy retryPolicy) {
    this.position = position;
    this.schemaObject = Objects.requireNonNull(schemaObject, "schemaObject cannot be null");
    this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy cannot be null");
  }

  public Uni<SubT> execute(
      CommandQueryExecutor queryExecutor, DriverExceptionHandler<SchemaT> exceptionHandler) {

    LOGGER.debug("OperationAttempt: execute starting for for attemptId {}", attemptId);

    // First things first: did we already fail? If so, propagate
    if (status() == OperationStatus.ERROR) {
      LOGGER.debug(
          "OperationAttempt: state was {}, propagating error for attemptId {}",
          status(),
          attemptId);

      if (failure == null) {
        var msg =
            String.format(
                "OperationAttempt state was %s, but failure throwable is null, attemptId %s",
                status(), attemptId);
        throw new IllegalStateException(msg);
      }
      return Uni.createFrom().failure(failure);
    }

    swapStatus("start execute()", OperationStatus.READY, OperationStatus.IN_PROGRESS);

    try {
      return execute(queryExecutor)
          .onFailure(throwable -> trackAndDecideRetry(exceptionHandler, throwable))
          .recoverWithUni(throwable -> recoveryUni(queryExecutor))
          .onFailure(throwable -> trackAndDecideRetry(exceptionHandler, throwable))
          .retry()
          .atMost(retryPolicy.maxRetries())
          .onItem()
          .transform(this::onSuccess);

    } catch (RuntimeException re) {
      // This is here to catch an in the execute() method of the subclass, when that happens there
      // is no
      // Uni for the rest of the function chain to run on.
      return Uni.createFrom().item(maybeAddFailure(exceptionHandler.maybeHandle(schemaObject, re)));
    }
  }

  protected abstract Uni<AsyncResultSet> execute(CommandQueryExecutor queryExecutor);

  protected boolean trackAndDecideRetry(
      DriverExceptionHandler<SchemaT> exceptionHandler, Throwable throwable) {

    maybeAddThrowable(exceptionHandler, throwable);
    return retryPolicy.shouldRetry(throwable);
  }

  protected void maybeAddThrowable(
      DriverExceptionHandler<SchemaT> exceptionHandler, Throwable throwable) {
    if (throwable instanceof RuntimeException re) {
      maybeAddFailure(exceptionHandler.maybeHandle(schemaObject, re));
    } else {
      maybeAddFailure(throwable);
    }
  }

  protected Uni<AsyncResultSet> recoveryUni(CommandQueryExecutor queryExecutor) {
    // HACK TODO: remove constants
    return Uni.createFrom()
        .deferred(() -> execute(queryExecutor))
        .onItem()
        .delayIt()
        .by(retryPolicy.delay); // operationsConfig.databaseConfig().ddlRetryDelayMillis()
  }

  /**
   * set compled and handle the reslt set is there
   *
   * @param resultSet
   * @return
   */
  protected SubT onSuccess(AsyncResultSet resultSet) {
    return setStatus(OperationStatus.COMPLETED);
  }

  @SuppressWarnings("unchecked")
  protected SubT downcast() {
    return (SubT) this;
  }

  public OperationStatus status() {
    return status;
  }

  /**
   * @param newStatus
   * @return The subtype of the OperationAttempt, useful when setting status is the last thing to do
   *     in a method chain.
   */
  protected SubT setStatus(OperationStatus newStatus) {
    LOGGER.debug(
        "OperationAttempt: status changing from {} to {} for attemptId {}",
        status(),
        newStatus,
        attemptId);
    status = newStatus;
    return downcast();
  }

  protected SubT checkStatus(String context, OperationStatus... expectedStatus) {
    if (Arrays.stream(expectedStatus).anyMatch(current -> current.equals(status()))) {
      return downcast();
    }

    var errorMsg =
        String.format(
            "OperationAttempt: checkStatus failed for %s, expected %s but actual %s for attemptId %s",
            context,
            String.join(",", Arrays.stream(expectedStatus).map(OperationStatus::name).toList()),
            status(),
            attemptId);
    throw new IllegalStateException(errorMsg);
  }

  protected SubT swapStatus(
      String context, OperationStatus expectedStatus, OperationStatus newStatus) {
    checkStatus(context, expectedStatus);
    return setStatus(newStatus);
  }

  public SubT setSkippedIfReady() {
    if (status() == OperationStatus.READY) {
      return setStatus(OperationAttempt.OperationStatus.SKIPPED);
    }
    return downcast();
  }

  public SubT checkTerminal() {

    if (status() == OperationStatus.READY) {
      return setStatus(OperationAttempt.OperationStatus.SKIPPED);
    }
    if (status().isTerminal()) {
      return downcast();
    }
    throw new IllegalStateException(
        String.format("OperationAttempt: checkTerminal failed, non terminal status %s", status()));
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

  /**
   * The first error that happened trying to run this insert.
   *
   * @return
   */
  public Optional<Throwable> failure() {
    return Optional.ofNullable(failure);
  }

  /**
   * Updates the attempt with an error that happened when trying to process the insert.
   *
   * <p>Implementations must only remember the first error that happened.
   *
   * @param failure An error that happened when trying to process the insert.
   * @return Return the updated {@link InsertAttempt}, must be the same instance the method was
   *     called on.
   */
  public SubT maybeAddFailure(Throwable failure) {
    if (this.failure == null) {
      this.failure = failure;
      LOGGER.warn("OperationAttempt: maybeAddFailure for attemptId {}", attemptId, failure);
    }
    if (this.failure != null) {
      setStatus(OperationStatus.ERROR);
    }
    return downcast();
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

  protected static class RetryPolicy {

    static final RetryPolicy NO_RETRY = new RetryPolicy();

    private final int maxRetries;
    private final Duration delay;

    private RetryPolicy() {
      this(1, Duration.ZERO);
    }

    RetryPolicy(int maxRetries, Duration delay) {
      // This is a requirement of UniRetryAtMost that is it >= 1, however UniRetry.atMost() says it
      // must be >= 0
      if (maxRetries < 1) {
        throw new IllegalArgumentException("maxRetries must be >= 1");
      }
      this.maxRetries = maxRetries;
      this.delay = delay;
    }

    int maxRetries() {
      return maxRetries;
    }

    Duration delay() {
      return delay;
    }

    boolean shouldRetry(Throwable throwable) {
      return false;
    }
  }
}