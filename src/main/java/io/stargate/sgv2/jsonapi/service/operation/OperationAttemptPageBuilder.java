package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.function.Supplier;

/**
 * After processing the {@link OperationAttempt}'s are grouped together into a page and then a
 * {@link CommandResult} is built from the page.
 *
 * <p>The {@link GenericOperation} a {@link OperationAttemptPageBuilder} that supports accumulating
 * the completed attempts and then building a page of results from them.
 *
 * <p>This is a base for a builder, provides some re-usable logic for how the should be build and
 * the interface the {@link GenericOperation} expects.
 */
public abstract class OperationAttemptPageBuilder<
        SchemaT extends SchemaObject, AttemptT extends OperationAttempt<AttemptT, SchemaT>>
    extends OperationAttemptAccumulator<SchemaT, AttemptT> {

  protected boolean useErrorObjectV2 = false;
  protected boolean debugMode = false;

  /**
   * Called ot turn the accumulated attempts into a {@link CommandResult}.
   *
   * @return A supplier that will provide the {@link CommandResult} when called, normally a sublcass
   *     of {@link OperationAttemptPage}
   */
  public abstract Supplier<CommandResult> getOperationPage();

  /**
   * Sets if the error object v2 formatting should be used when building the {@link CommandResult}.
   */
  @SuppressWarnings("unchecked")
  public <SubT extends OperationAttemptPageBuilder<SchemaT, AttemptT>> SubT useErrorObjectV2(
      boolean useErrorObjectV2) {
    this.useErrorObjectV2 = useErrorObjectV2;
    return (SubT) this;
  }

  /**
   * Set if API is running in debug mode, this adds additional info to the response. See {@link
   * io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder}.
   */
  @SuppressWarnings("unchecked")
  public <SubT extends OperationAttemptPageBuilder<SchemaT, AttemptT>> SubT debugMode(
      boolean debugMode) {
    this.debugMode = debugMode;
    return (SubT) this;
  }
}
