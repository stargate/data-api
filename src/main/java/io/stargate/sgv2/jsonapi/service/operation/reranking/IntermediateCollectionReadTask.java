package io.stargate.sgv2.jsonapi.service.operation.reranking;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.TraceMessage;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingTaskBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tasks.BaseTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import io.stargate.sgv2.jsonapi.service.resolver.FindCommandResolver;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntermediateCollectionReadTask
    extends BaseTask<
        CollectionSchemaObject,
        IntermediateCollectionReadTask.IntermediateReadResultSupplier,
        IntermediateCollectionReadTask.IntermediateReadResults> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IntermediateCollectionReadTask.class);

  private final FindCommandResolver findCommandResolver;
  private final FindCommand findCommand;
  private final DeferredVectorize deferredVectorize;
  private final DeferredCommandResultAction commandResultAction;

  public IntermediateCollectionReadTask(
      int position,
      CollectionSchemaObject schemaObject,
      TaskRetryPolicy retryPolicy,
      FindCommandResolver findCommandResolver,
      FindCommand findCommand,
      DeferredVectorize deferredVectorize,
      DeferredCommandResultAction commandResultAction) {
    super(position, schemaObject, retryPolicy);

    this.findCommandResolver = findCommandResolver;
    this.findCommand = findCommand;
    this.deferredVectorize = deferredVectorize;
    this.commandResultAction = commandResultAction;

    setStatus(TaskStatus.READY);
  }

  public static <SchemaT extends TableBasedSchemaObject> EmbeddingTaskBuilder<SchemaT> builder(
      CommandContext<SchemaT> commandContext) {
    return new EmbeddingTaskBuilder<>(commandContext);
  }

  // =================================================================================================
  // BaseTask overrides
  // =================================================================================================

  @Override
  protected IntermediateReadResultSupplier buildResultSupplier(
      CommandContext<CollectionSchemaObject> commandContext) {

    // If we have a deferred vectroize, we should use it to update the sort clause on the find
    // command
    if (deferredVectorize != null) {
      findCommand.sortClause().sortExpressions().clear();
      // will throw if the deferred value is not complete
      findCommand
          .sortClause()
          .sortExpressions()
          .add(SortExpression.vsearch(deferredVectorize.getVector()));
    }

    Operation<CollectionSchemaObject> findOperation =
        findCommandResolver.resolveCommand(commandContext, findCommand);
    return new IntermediateReadResultSupplier(
        () -> {
          commandContext
              .requestTracing()
              .maybeTrace(
                  () ->
                      new TraceMessage(
                          "Executing inner '%s' command for schema object '%s' "
                              .formatted(
                                  findCommand.commandName().getApiName(), schemaObject.name()),
                          Recordable.copyOf(Map.of("command", findCommand))));
          return findOperation.execute(commandContext);
        },
        List.of(commandResultAction));
  }

  @Override
  protected RuntimeException maybeHandleException(
      IntermediateReadResultSupplier resultSupplier, RuntimeException runtimeException) {
    return runtimeException;
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return super.recordTo(dataRecorder)
        .append("deferredVectorize isNull", deferredVectorize == null)
        .append(
            "sortClause.sortExpression.paths",
            findCommand.sortClause().sortExpressions().stream().map(SortExpression::path).toList());
  }

  // =================================================================================================
  // Implementation and internals
  // =================================================================================================

  public static class IntermediateReadResultSupplier
      implements UniSupplier<IntermediateReadResults> {

    private final Supplier<Uni<Supplier<CommandResult>>> opResultSupplier;
    private final List<DeferredCommandResultAction> actions;

    IntermediateReadResultSupplier(
        Supplier<Uni<Supplier<CommandResult>>> opResultSupplier,
        List<DeferredCommandResultAction> actions) {
      this.opResultSupplier = opResultSupplier;
      this.actions = actions;
    }

    @Override
    public Uni<IntermediateReadResults> get() {
      return opResultSupplier
          .get()
          .onItem()
          .transform(Supplier::get)
          .onItem()
          .transform(commandResult -> IntermediateReadResults.create(commandResult, actions));
    }
  }

  public static class IntermediateReadResults {

    private final CommandResult commandResult;
    private final List<DeferredCommandResultAction> actions;

    private IntermediateReadResults(
        CommandResult commandResult, List<DeferredCommandResultAction> actions) {
      this.commandResult = commandResult;
      this.actions = actions;
    }

    /**
     * Delivers the {@link CommandResult} and the {@link DeferredCommandResultAction}s waiting for
     * them.
     *
     * @param commandResult The command result to deliver.
     * @param actions The actions that were waiting for the command result.
     * @return IntermediateReadResults with the command result and the actions it was delivered to.
     */
    static IntermediateReadResults create(
        CommandResult commandResult, List<DeferredCommandResultAction> actions) {
      // factory that exits just to avoid doing work that may throw in the constructor

      actions.forEach(action -> action.onSuccess(commandResult));
      return new IntermediateReadResults(commandResult, actions);
    }
  }
}
