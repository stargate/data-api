package io.stargate.sgv2.jsonapi.service.operation;

import io.smallrye.mutiny.Multi;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

/**
 * Provides a base implementation for accumulating {@link OperationAttempt}s that the {@link
 * GenericOperation} calls when using hte {@link Multi#collect()} method.
 *
 * <p>This is here so the {@link GenericOperation} has a generic way to accumulate the attempts that
 * it will run, look at {@link OperationAttemptPageBuilder} for how to it is used.
 */
public abstract class OperationAttemptAccumulator<
    SchemaT extends SchemaObject, AttemptT extends OperationAttempt<AttemptT, SchemaT>> {

  protected final OperationAttemptContainer<SchemaT, AttemptT> attempts =
      new OperationAttemptContainer<>();

  protected OperationAttemptAccumulator() {}

  public void accumulate(AttemptT attempt) {
    attempts.add(attempt);
  }
}
