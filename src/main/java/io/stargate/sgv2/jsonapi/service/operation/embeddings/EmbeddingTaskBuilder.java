package io.stargate.sgv2.jsonapi.service.operation.embeddings;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import java.util.List;
import java.util.Objects;

public class EmbeddingTaskBuilder
    extends TaskBuilder<EmbeddingTask<TableSchemaObject>, TableSchemaObject> {

  private VectorizeDefinition vectorizeDefinition;
  private List<EmbeddingAction> embeddingActions;
  private TaskRetryPolicy retryPolicy = null;

  public EmbeddingTaskBuilder(TableSchemaObject schemaObject) {
    super(schemaObject);
  }

  public EmbeddingTaskBuilder withVectorizeDefinition(VectorizeDefinition vectorizeDefinition) {
    this.vectorizeDefinition = vectorizeDefinition;
    return this;
  }

  public EmbeddingTaskBuilder withEmbeddingActions(List<EmbeddingAction> embeddingActions) {
    this.embeddingActions = embeddingActions;
    return this;
  }

  public EmbeddingTaskBuilder withRetryPolicy(TaskRetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
    return this;
  }

  public EmbeddingTask<TableSchemaObject> build() {
    Objects.requireNonNull(vectorizeDefinition, "vectorizeDefinition cannot be null");
    Objects.requireNonNull(embeddingActions, "embeddingActions cannot be null");
    Objects.requireNonNull(retryPolicy, "retryPolicy cannot be null");

    return new EmbeddingTask<>(
        nextPosition(), schemaObject, retryPolicy, vectorizeDefinition, embeddingActions);
  }
}
