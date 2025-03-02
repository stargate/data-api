package io.stargate.sgv2.jsonapi.service.operation.embeddings;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
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
 * Builder to create a group of {@link EmbeddingTask}s from a list of {@link EmbeddingAction}s.
 *
 * <p>Such as when there is an insert operation that needs to be vectorized, and the embedding done
 * before the inserts in a {@link io.stargate.sgv2.jsonapi.service.operation.tasks.CompositeTask}
 */
public class EmbeddingTaskGroupBuilder<SchemaT extends TableBasedSchemaObject> {
  private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddingTaskGroupBuilder.class);

  private CommandContext<SchemaT> commandContext;
  private List<EmbeddingAction> embeddingActions;
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
      List<EmbeddingAction> embeddingActions) {
    this.embeddingActions = embeddingActions;
    return this;
  }

  public TaskGroup<EmbeddingTask<SchemaT>, SchemaT> build() {
    Objects.requireNonNull(embeddingActions, "embeddingActions cannot be null");

    if (embeddingActions.isEmpty()) {
      throw new IllegalArgumentException("embeddingActions is empty, nothing to do");
    }

    // grouping the actions by the calls that need to be made
    Map<EmbeddingAction.EmbeddingActionGroupKey, List<EmbeddingAction>> actionGroups =
        embeddingActions.stream().collect(Collectors.groupingBy(EmbeddingAction::groupKey));
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "build() - building embedding task group, actionGroups.size: {}, tasks: {}",
          actionGroups.size(),
          PrettyPrintable.pprint(Recordable.copyOf(actionGroups)));
    }

    // UPTO: ARON = TRACE the actions

    // each group of embedding actions is a single Embedding Task
    // and we will run them in parallel
    TaskGroup<EmbeddingTask<SchemaT>, SchemaT> taskGroup = new TaskGroup<>(false);

    // maybe the EmbeddingTask.builder is not needed any  more, but it was made first and is
    // there to build a single task
    actionGroups.forEach(
        (groupKey, groupActions) -> {
          var embeddingTask =
              EmbeddingTask.builder(commandContext)
                  .withApiVectorType(groupKey.vectorType())
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
