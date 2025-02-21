package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import java.util.function.Supplier;

/** Task that runs an operation */
public class CompositeTask<SchemaT extends TableBasedSchemaObject>
    extends BaseTask<SchemaT, BaseTask.UniSupplier<CommandResult>, CommandResult> {

  public static final Supplier<CommandResult> NULL_COMMAND_MARKER =
      () -> {
        throw new IllegalStateException("CommandResult not set");
      };

  private final Operation<SchemaT> innerOperation;
  private CommandResult innerOperationResult = null;

  public CompositeTask(
      int position,
      SchemaT schemaObject,
      TaskRetryPolicy retryPolicy,
      Operation<SchemaT> innerOperation) {
    super(position, schemaObject, retryPolicy);

    this.innerOperation = innerOperation;
  }

  public static <TaskT extends CompositeTask<SchemaT>, SchemaT extends TableBasedSchemaObject>
      CompositeTaskAccumulator<TaskT, SchemaT> accumulator(
          Class<TaskT> taskClass, CommandContext<SchemaT> commandContext) {
    return TaskAccumulator.configureForContext(new CompositeTaskAccumulator<>(), commandContext);
  }

  @Override
  protected UniSupplier<CommandResult> buildResultSupplier(CommandContext<SchemaT> commandContext) {
    // Operation.execute() returns Uni<Supplier<CommandResult>>
    // but we want Supplier<Uni<ResulT>> so we need to transform it
    return () -> innerOperation.execute(commandContext).onItem().transform(Supplier::get);
  }

  @Override
  protected RuntimeException maybeHandleException(
      UniSupplier<CommandResult> resultSupplier, RuntimeException runtimeException) {
    return runtimeException;
  }

  @Override
  protected void onSuccess(CommandResult result) {
    // calling super to make sure state is set.
    super.onSuccess(result);

    // This is the result of the operation, the composite task accumulator may want this to return
    // to the user
    innerOperationResult = result;
  }

  // may be null
  protected CommandResult getInnerOperationResult() {
    return innerOperationResult;
  }
}
