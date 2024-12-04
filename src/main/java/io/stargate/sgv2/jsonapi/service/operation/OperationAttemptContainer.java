package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.*;

/**
 * Ordered container of {@link OperationAttempt} objects that are part of the same {@link
 * io.stargate.sgv2.jsonapi.api.model.command.Command}.
 *
 * <p>The container can be configured to process the attempts sequentially or in parallel (default),
 * and other config that is needed about how to process the attempts as a group should live here
 * (e.g. if there is a delay between attempts, or if the container should fail fast).
 *
 * @param <SchemaT> Schema object type
 * @param <AttemptT> Operation attempt type
 */
public class OperationAttemptContainer<
        SchemaT extends SchemaObject, AttemptT extends OperationAttempt<AttemptT, SchemaT>>
    extends ArrayList<AttemptT> {

  private boolean sequentialProcessing = false;
  private final UUID containerId = UUID.randomUUID();

  public OperationAttemptContainer() {
    super();
  }

  /**
   * Initialize the container.
   *
   * @param sequentialProcessing If true the attempts will be processed sequentially, rather than in
   *     parallel, and the container will skip any remaining attempts if one fails. If false the
   *     attempts will be processed in parallel and all attempts will be processed regardless of the
   *     status of any other attempts.
   */
  public OperationAttemptContainer(boolean sequentialProcessing) {
    super();
    this.sequentialProcessing = sequentialProcessing;
  }

  public OperationAttemptContainer(AttemptT attempt) {
    this(List.of(attempt));
  }

  public OperationAttemptContainer(List<AttemptT> attempts) {
    super(attempts);
  }

  /**
   * See {@link #OperationAttemptContainer(boolean)}.
   *
   * @return If the container will process the attempts sequentially.
   */
  public boolean getSequentialProcessing() {
    return sequentialProcessing;
  }

  /**
   * See {@link #OperationAttemptContainer(boolean)}.
   *
   * @param sequentialProcessing If the container will process the attempts sequentially.
   */
  public void setSequentialProcessing(boolean sequentialProcessing) {
    this.sequentialProcessing = sequentialProcessing;
  }

  public List<AttemptT> errorAttempts() {
    return stream()
        .filter(attempt -> attempt.status() == OperationAttempt.OperationStatus.ERROR)
        .toList();
  }

  public boolean allAttemptsCompleted() {
    return stream()
        .allMatch(attempt -> attempt.status() == OperationAttempt.OperationStatus.COMPLETED);
  }

  public List<AttemptT> completedAttempts() {
    return stream()
        .filter(attempt -> attempt.status() == OperationAttempt.OperationStatus.COMPLETED)
        .toList();
  }

  public List<AttemptT> skippedAttempts() {
    return stream()
        .filter(attempt -> attempt.status() == OperationAttempt.OperationStatus.SKIPPED)
        .toList();
  }

  public void throwIfNotAllTerminal() {

    var nonTerminalAttempts = stream().filter(attempt -> !attempt.status().isTerminal()).toList();

    if (!nonTerminalAttempts.isEmpty()) {
      var msg = String.join(", ", nonTerminalAttempts.stream().map(Object::toString).toList());
      throw new IllegalStateException(
          "throwIfNotAllTerminal() - Non terminal OperationAttempts found, non-terminal=" + msg);
    }
  }

  /**
   * Since container is arrayList that preserves order, if any attempt before(include) targetAttempt
   * has error status, then targetAttempt should failFast. Container should fail fast and stop
   * processing any further attempts.
   *
   * @return <code>true</code> if the container is configured for sequential processing and there is
   *     at least one error before(include) the targetAttempt
   */
  public boolean shouldFailFast(AttemptT targetAttempt) {
    if (!sequentialProcessing) {
      return false;
    }
    for (AttemptT attempt : this) {
      if (attempt == targetAttempt) {
        return targetAttempt.status() == OperationAttempt.OperationStatus.ERROR;
      }
      if (attempt.status() == OperationAttempt.OperationStatus.ERROR) {
        return true; // Fail fast if any prior attempt is an error
      }
    }
    return false;
  }

  @Override
  public String toString() {
    Map<OperationAttempt.OperationStatus, Integer> statusCount = new HashMap<>(size());
    forEach(attempt -> statusCount.merge(attempt.status(), 1, Math::addExact));

    return new StringBuilder("OperationAttemptContainer{")
        .append("containerId=")
        .append(containerId)
        .append(", sequentialProcessing=")
        .append(sequentialProcessing)
        .append(", count=")
        .append(size())
        .append(", statusCount=")
        .append(statusCount)
        .append('}')
        .toString();
  }
}
