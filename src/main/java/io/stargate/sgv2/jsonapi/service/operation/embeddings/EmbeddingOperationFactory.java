package io.stargate.sgv2.jsonapi.service.operation.embeddings;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.tasks.*;
import io.stargate.sgv2.jsonapi.service.shredding.Deferrable;
import io.stargate.sgv2.jsonapi.service.shredding.ValueAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates repeated logic for creating a composite task group when we have deferred values that
 * need to be vectorized.
 *
 * <p>We do this in the Insert, Read, and Update paths
 */
public abstract class EmbeddingOperationFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddingOperationFactory.class);

  private EmbeddingOperationFactory() {}

  public static <TaskT extends Task<TableSchemaObject>> Operation<TableSchemaObject> maybeEmbedding(
      CommandContext<TableSchemaObject> commandContext,
      TaskGroupAndDeferrables<TaskT, TableSchemaObject> tasksAndDeferrables) {

    var allDeferredValues = Deferrable.deferredValues(tasksAndDeferrables.deferrables());
    if (allDeferredValues.isEmpty()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "build() - zero deferred values, creating direct TaskOperation operation tasksAndDeferrables.size={}",
            tasksAndDeferrables.taskGroup().size());
      }

      // basic update, just wrap the tasks in an operation and go
      return new TaskOperation<>(
          tasksAndDeferrables.taskGroup(), tasksAndDeferrables.accumulator());
    }

    // we have some deferred values, e.g. we need to do vectorizing, so we need  to build a
    // hierarchy of task groups for now we only have vectorize actions so quick sanity check
    var allActions = ValueAction.filteredActions(ValueAction.class, allDeferredValues);
    var embeddingActions = ValueAction.filteredActions(EmbeddingAction.class, allDeferredValues);
    if (allActions.size() != embeddingActions.size()) {
      throw new IllegalArgumentException("Unsupported actions in deferred values: " + allActions);
    }

    var compositeBuilder = new CompositeTaskOperationBuilder<>(commandContext);

    // Send the EmbeddingAction's to the builder to get back a list of EmbeddingTasks
    // that are linked to the actions they get vectors for.
    var embeddingTaskGroup =
        new EmbeddingTaskGroupBuilder<TableSchemaObject>()
            .withCommandContext(commandContext)
            .withEmbeddingActions(embeddingActions)
            .withRequestType(EmbeddingProvider.EmbeddingRequestType.SEARCH)
            .build();
    compositeBuilder.withIntermediateTasks(embeddingTaskGroup, TaskRetryPolicy.NO_RETRY);

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "build() - deferred values for vectorizing, returning composite task group with embeddingTaskGroup.size={}, tasksAndDeferrables.size={}",
          embeddingTaskGroup.size(),
          tasksAndDeferrables.taskGroup().size());
    }

    // we want to run a group of embedding tasks and then a group of other tasks,
    // the two groups are linked by the EmbeddingAction objects
    // Because these are tables we only use the driver retry for the inserts, not task level retry
    return compositeBuilder.build(
        tasksAndDeferrables.taskGroup(),
        TaskRetryPolicy.NO_RETRY,
        tasksAndDeferrables.accumulator());
  }
}
