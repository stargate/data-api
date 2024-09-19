package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
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
    SKIPPED
  }

  private final int position;
  protected final SchemaT schemaObject;
  protected final UUID attemptId = UUID.randomUUID();
  private Throwable failure;

  // Keep this private, so sub-classes set through setter incase we need to syncronize later
  private OperationStatus status = OperationStatus.UNINITIALIZED;

  protected OperationAttempt(int position, SchemaT schemaObject) {
    this.position = position;
    this.schemaObject = Objects.requireNonNull(schemaObject, "schemaObject cannot be null");
  }

  public Uni<SubT> execute(
      DataApiRequestInfo dataApiRequestInfo,
      QueryExecutor queryExecutor,
      DriverExceptionHandler<SchemaT> exceptionHandler) {
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

    // TODO: we will eventually stop needing to hand the DataApiRequestInfo around
    var session = queryExecutor.getCqlSessionCache().getSession(dataApiRequestInfo);

    return Uni.createFrom()
        .completionStage(execute(session))
        .onItemOrFailure()
        .transform(
            (result, throwable) ->
                switch (throwable) {
                  case null -> setStatus(OperationStatus.COMPLETED);
                  case RuntimeException re ->
                      maybeAddFailure(exceptionHandler.maybeHandle(schemaObject, re));
                  default -> maybeAddFailure(throwable);
                });
  }

  protected abstract CompletionStage<AsyncResultSet> execute(CqlSession session);

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

  public SubT verifyComplete() {

    if (status() == OperationStatus.READY) {
      return setStatus(OperationAttempt.OperationStatus.SKIPPED);
    }

    return checkStatus(
        "verifyComplete()",
        OperationAttempt.OperationStatus.ERROR,
        OperationAttempt.OperationStatus.COMPLETED);
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
    }
    if (this.failure != null) {
      setStatus(OperationStatus.ERROR);
    }
    return downcast();
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
}
