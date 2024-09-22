package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

public abstract class OperationAttemptAccumulator<
    SchemaT extends SchemaObject, AttemptT extends OperationAttempt<AttemptT, SchemaT>> {

  protected final OperationAttemptContainer<SchemaT, AttemptT> attempts =
      new OperationAttemptContainer<>();

  protected OperationAttemptAccumulator() {}

  public void accumulate(AttemptT attempt) {
    attempts.add(attempt);
  }
}
