package io.stargate.sgv2.jsonapi.service.operation.reranking;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingTaskBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tasks.BaseTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import io.stargate.sgv2.jsonapi.service.resolver.FindCommandResolver;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class IntermediateCollectionReadTask
    extends BaseTask<
    CollectionSchemaObject, IntermediateCollectionReadTask.IntermediateReadResultSupplier, IntermediateCollectionReadTask.IntermediateReadResults> {

  private static final Logger LOGGER = LoggerFactory.getLogger(IntermediateCollectionReadTask.class);

  public IntermediateCollectionReadTask(
      int position,
      CollectionSchemaObject schemaObject,
      TaskRetryPolicy retryPolicy,
      FindCommandResolver findCommandResolver,
      FindCommand findCommand,
      DeferredVectorize deferredVectorize,
      DeferredCommandResultAction commandResultAction) {
    super(position, schemaObject, retryPolicy);

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
    return null;
  }

  @Override
  protected RuntimeException maybeHandleException(
      IntermediateReadResultSupplier resultSupplier, RuntimeException runtimeException) {
    return runtimeException;
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return super.recordTo(dataRecorder);
  }

  // =================================================================================================
  // Implementation and internals
  // =================================================================================================

  public static class IntermediateReadResultSupplier implements UniSupplier<IntermediateReadResults> {


    IntermediateReadResultSupplier() {
    }

    @Override
    public Uni<IntermediateReadResults> get() {
      return null;
    }
  }

  public static class IntermediateReadResults {

    protected final List<DeferredCommandResultAction> actions;

    private IntermediateReadResults(List<CommandResult> commandResults, List<DeferredCommandResultAction> actions) {
      this.actions = actions;
    }
  }
}
