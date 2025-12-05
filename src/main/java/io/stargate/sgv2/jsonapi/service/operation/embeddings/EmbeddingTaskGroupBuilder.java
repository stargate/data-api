package io.stargate.sgv2.jsonapi.service.operation.embeddings;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableBasedSchemaObject;import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builder to create a group of {@link EmbeddingTask}s from a list of {@link
 * EmbeddingDeferredAction}s.
 *
 * <p>Handles needing to make multiple embedding calls by looking at the {@link
 * EmbeddingAction#groupKey()}
 */
public class EmbeddingTaskGroupBuilder<SchemaT extends TableBasedSchemaObject> {
  private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddingTaskGroupBuilder.class);

  private CommandContext<SchemaT> commandContext;
  private List<EmbeddingDeferredAction> embeddingActions;
  private EmbeddingProvider.EmbeddingRequestType requestType;

  public EmbeddingTaskGroupBuilder<SchemaT> withCommandContext(
      CommandContext<SchemaT> commandContext) {
    this.commandContext = commandContext;
    return this;
  }

  public EmbeddingTaskGroupBuilder<SchemaT> withRequestType(
      EmbeddingProvider.EmbeddingRequestType requestType) {
    this.requestType = requestType;
    return this;
  }

  public EmbeddingTaskGroupBuilder<SchemaT> withEmbeddingActions(
      List<EmbeddingDeferredAction> embeddingActions) {
    this.embeddingActions = embeddingActions;
    return this;
  }

  public TaskGroup<EmbeddingTask<SchemaT>, SchemaT> build() {
    Objects.requireNonNull(embeddingActions, "embeddingActions cannot be null");
    Objects.requireNonNull(requestType, "requestType cannot be null");

    if (embeddingActions.isEmpty()) {
      throw new IllegalArgumentException("embeddingActions is empty, nothing to do");
    }

    // grouping the actions by the calls that need to be made
    Map<EmbeddingDeferredAction.EmbeddingActionGroupKey, List<EmbeddingDeferredAction>>
        actionGroups =
            embeddingActions.stream()
                .collect(Collectors.groupingBy(EmbeddingDeferredAction::groupKey));
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "build() - building embedding task group, actionGroups.size: {}, actionGroups: {}",
          actionGroups.size(),
          PrettyPrintable.print(Recordable.copyOf(actionGroups)));
    }

    // each group of embedding actions is a single Embedding Task, they group the provider etc.
    // and we will run them in parallel
    TaskGroup<EmbeddingTask<SchemaT>, SchemaT> taskGroup = new TaskGroup<>(false);

    actionGroups.forEach(
        (groupKey, groupActions) -> {
          var embeddingTask =
              EmbeddingTask.builder(commandContext)
                  .withDimension(groupKey.dimension())
                  .withVectorizeDefinition(groupKey.vectorizeDefinition())
                  .withEmbeddingActions(groupActions)
                  .withRetryPolicy(TaskRetryPolicy.NO_RETRY)
                  .withOriginalCommandName(commandContext.commandName())
                  .withRequestType(requestType)
                  .build();
          taskGroup.add(embeddingTask);
        });
    return taskGroup;
  }
}
