package io.stargate.sgv2.jsonapi.service.operation.embeddings;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorType;
import java.util.List;
import java.util.Objects;

/**
 * Builds a {@link EmbeddingTask}, normally use the {@link EmbeddingTaskGroupBuilder} because it can
 * handle needing multiple tasks for multiple embedding calls.
 *
 * <p>aaron - march 19 2025 - this is not adding a lot other than knowing how to get the
 * EmbeddingProvider , but it uses the TaskBuilder where we will add more things for the task
 * positions (complex in hierarchies) and manage creating the retry policy.
 *
 * @param <SchemaT>
 */
public class EmbeddingTaskBuilder<SchemaT extends TableBasedSchemaObject>
    extends TaskBuilder<EmbeddingTask<SchemaT>, SchemaT, EmbeddingTaskBuilder<SchemaT>> {

  private final CommandContext<SchemaT> commandContext;
  // aaron 22 feb 2025 - for unknown reasons we need the command name when we create the embedding
  // provider
  private String originalCommandName;
  private ApiVectorType apiVectorType;
  private List<EmbeddingAction> embeddingActions;
  private TaskRetryPolicy retryPolicy = null;
  private EmbeddingProvider.EmbeddingRequestType requestType;

  public EmbeddingTaskBuilder(CommandContext<SchemaT> commandContext) {
    super(commandContext.schemaObject());
    this.commandContext = commandContext;
  }

  public EmbeddingTaskBuilder<SchemaT> withApiVectorType(ApiVectorType apiVectorType) {
    this.apiVectorType = apiVectorType; // note changes in a later PR to use the vectorizeDefinition
    return this;
  }

  public EmbeddingTaskBuilder<SchemaT> withEmbeddingActions(
      List<EmbeddingAction> embeddingActions) {
    this.embeddingActions = embeddingActions;
    return this;
  }

  public EmbeddingTaskBuilder<SchemaT> withRetryPolicy(TaskRetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
    return this;
  }

  public EmbeddingTaskBuilder<SchemaT> withOriginalCommandName(String originalCommandName) {
    this.originalCommandName = originalCommandName;
    return this;
  }

  public EmbeddingTaskBuilder<SchemaT> withRequestType(
      EmbeddingProvider.EmbeddingRequestType requestType) {
    this.requestType = requestType;
    return this;
  }

  public EmbeddingTask<SchemaT> build() {
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
