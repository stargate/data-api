package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OperationAttemptContainer<
        SchemaT extends SchemaObject, AttemptT extends OperationAttempt<AttemptT, SchemaT>>
    extends ArrayList<AttemptT> {

  private boolean sequentialProcessing = false;

  public OperationAttemptContainer() {
    super();
  }

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

  public boolean getSequentialProcessing() {
    return sequentialProcessing;
  }

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
          "checkAllAttemptsTerminal() - Non terminal OperationAttempts found, non-terminal=" + msg);
    }
  }

  public boolean shouldFailFast() {
    if (!sequentialProcessing) {
      return false;
    }
    return stream().anyMatch(attempt -> attempt.status() == OperationAttempt.OperationStatus.ERROR);
  }

  @Override
  public String toString() {
    Map<OperationAttempt.OperationStatus, Integer> statusCount = new HashMap<>(size());
    forEach(attempt -> statusCount.merge(attempt.status(), 1, Math::addExact));

    return new StringBuilder("OperationAttemptContainer{")
        .append("sequentialProcessing=")
        .append(sequentialProcessing)
        .append(", count=")
        .append(size())
        .append(", statusCount=")
        .append(statusCount)
        .append('}')
        .toString();
  }
}
