package io.stargate.sgv2.jsonapi.service.operation.embeddings;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.operation.tasks.BaseTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskAccumulator;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import java.util.List;

public class EmbeddingTask<SchemaT extends SchemaObject>
    extends BaseTask<
        SchemaT, EmbeddingTask.EmbeddingResultSupplier, EmbeddingTask.EmbeddingResult> {

  private final VectorizeDefinition vectorizeDefinition;
  private final List<EmbeddingAction> embeddingActions;

  protected EmbeddingTask(
      int position,
      SchemaT schemaObject,
      TaskRetryPolicy retryPolicy,
      VectorizeDefinition vectorizeDefinition,
      List<EmbeddingAction> embeddingActions) {
    super(position, schemaObject, retryPolicy);

    this.vectorizeDefinition = vectorizeDefinition;
    this.embeddingActions = embeddingActions;
  }

  public static EmbeddingTaskBuilder builder(CommandContext<TableSchemaObject> commandContext) {
    return new EmbeddingTaskBuilder(commandContext.schemaObject());
  }

  public static EbeddingTaskAccumulator<TableSchemaObject> accumulator(
      CommandContext<TableSchemaObject> commandContext) {
    return TaskAccumulator.configureForContext(new EbeddingTaskAccumulator<>(), commandContext);
  }

  @Override
  protected EmbeddingTask.EmbeddingResultSupplier buildResultSupplier(
      CommandContext<SchemaT> commandContext) {
    return null;
  }

  @Override
  protected RuntimeException maybeHandleException(
      EmbeddingResultSupplier resultSupplier, RuntimeException runtimeException) {
    return null;
  }

  public static class EmbeddingResultSupplier implements BaseTask.UniSupplier<EmbeddingResult> {

    public EmbeddingResultSupplier() {}

    @Override
    public Uni<EmbeddingResult> get() {
      return null;
    }
  }

  public record EmbeddingResult() {}
}
