package io.stargate.sgv2.jsonapi.service.operation.embeddings;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorType;
import java.util.List;
import java.util.Objects;

public class EmbeddingTaskBuilder
    extends TaskBuilder<EmbeddingTask<TableSchemaObject>, TableSchemaObject> {

  private CommandContext<TableSchemaObject> commandContext;
  // aaron 22 feb 2025 - for unknown we need the command name when we create the embedding provider
  private String originalCommandName;
  private ApiVectorType apiVectorType;
  private List<EmbeddingAction> embeddingActions;
  private TaskRetryPolicy retryPolicy = null;
  private EmbeddingProvider.EmbeddingRequestType requestType;

  public EmbeddingTaskBuilder(CommandContext<TableSchemaObject> commandContext) {
    super(commandContext.schemaObject());

    this.commandContext = commandContext;
  }

  public EmbeddingTaskBuilder withApiVectorType(ApiVectorType apiVectorType) {
    this.apiVectorType = apiVectorType;
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

  public EmbeddingTaskBuilder withOriginalCommandName(String originalCommandName) {
    this.originalCommandName = originalCommandName;
    return this;
  }

  public EmbeddingTaskBuilder withRequestType(EmbeddingProvider.EmbeddingRequestType requestType) {
    this.requestType = requestType;
    return this;
  }

  public EmbeddingTask<TableSchemaObject> build() {
    Objects.requireNonNull(apiVectorType, "apiVectorType cannot be null");
    Objects.requireNonNull(
        apiVectorType.getVectorizeDefinition(),
        "apiVectorType.getVectorizeDefinition() cannot be null");
    Objects.requireNonNull(embeddingActions, "embeddingActions cannot be null");
    Objects.requireNonNull(retryPolicy, "retryPolicy cannot be null");
    Objects.requireNonNull(originalCommandName, "originalCommand cannot be null");
    Objects.requireNonNull(requestType, "requestType cannot be null");

    var embeddingProvider =
        commandContext
            .embeddingProviderFactory()
            .getConfiguration(
                commandContext.requestContext().getTenantId(),
                commandContext.requestContext().getCassandraToken(),
                apiVectorType.getVectorizeDefinition().provider(),
                apiVectorType.getVectorizeDefinition().modelName(),
                apiVectorType.getDimension(),
                apiVectorType.getVectorizeDefinition().parameters(),
                apiVectorType.getVectorizeDefinition().authentication(),
                originalCommandName);

    return new EmbeddingTask<>(
        nextPosition(),
        schemaObject,
        retryPolicy,
        embeddingProvider,
        embeddingActions,
        requestType);
  }
}
