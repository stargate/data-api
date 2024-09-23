package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.function.Supplier;

public abstract class OperationAttemptPageBuilder<
        SchemaT extends SchemaObject, AttemptT extends OperationAttempt<AttemptT, SchemaT>>
    extends OperationAttemptAccumulator<SchemaT, AttemptT> {

  protected boolean useErrorObjectV2 = false;
  protected boolean debugMode = false;

  public abstract Supplier<CommandResult> getOperationPage();

  @SuppressWarnings("unchecked")
  public <SubT extends OperationAttemptPageBuilder<SchemaT, AttemptT>> SubT useErrorObjectV2(
      boolean useErrorObjectV2) {
    this.useErrorObjectV2 = useErrorObjectV2;
    return (SubT) this;
  }

  @SuppressWarnings("unchecked")
  public <SubT extends OperationAttemptPageBuilder<SchemaT, AttemptT>> SubT debugMode(
      boolean debugMode) {
    this.debugMode = debugMode;
    return (SubT) this;
  }
}
