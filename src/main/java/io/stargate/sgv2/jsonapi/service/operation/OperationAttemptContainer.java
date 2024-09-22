package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.ArrayList;
import java.util.List;

public class OperationAttemptContainer<
        SchemaT extends SchemaObject, AttemptT extends OperationAttempt<AttemptT, SchemaT>>
    extends ArrayList<AttemptT> {

  public OperationAttemptContainer() {
    super();
  }

  public OperationAttemptContainer(int initialCapacity) {
    super(initialCapacity);
  }

  public OperationAttemptContainer(List<AttemptT> attempts) {
    super(attempts);
  }

  public List<AttemptT> errorAttempts() {
    return stream()
        .filter(attempt -> attempt.status() == OperationAttempt.OperationStatus.ERROR)
        .toList();
  }

  public List<AttemptT> completedAttempts() {
    return stream()
        .filter(attempt -> attempt.status() == OperationAttempt.OperationStatus.COMPLETED)
        .toList();
  }

  public List<AttemptT> skippedAttempts() {
    return stream()
        .filter(attempt -> attempt.status() == OperationAttempt.OperationStatus.COMPLETED)
        .toList();
  }

  public void checkAllAttemptsTerminal() {
    var nonTerminalAttempts = stream().filter(attempt -> !attempt.status().isTerminal()).toList();
    if (!nonTerminalAttempts.isEmpty()) {
      var msg = String.join(", ", nonTerminalAttempts.stream().map(Object::toString).toList());
      throw new IllegalStateException(
          "checkAllAttemptsTerminal() - Non terminal OperationAttempts found, non-terminal=" + msg);
    }
  }
}
