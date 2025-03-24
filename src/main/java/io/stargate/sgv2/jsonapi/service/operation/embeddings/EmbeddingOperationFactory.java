package io.stargate.sgv2.jsonapi.service.operation.embeddings;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.tasks.*;
import io.stargate.sgv2.jsonapi.service.shredding.Deferrable;
import io.stargate.sgv2.jsonapi.service.shredding.DeferredAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the logic created a {@link CompositeTask} pipline when there are tasks that may need
 * embedding done via the {@link EmbeddingDeferredAction}.
 *
 * <p>We do this in the Insert, Read, and Update paths
 */
public abstract class EmbeddingOperationFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddingOperationFactory.class);

  private EmbeddingOperationFactory() {}

  /**
   * Creates ann {@link Operation} that will either run the tasks directly or run a group of {@link
   * CompositeTask} that include {@link EmbeddingTask}'s
   *
   * @param commandContext The command context
   * @param tasksAndDeferrables The group of tasks that have deferrable values, if the deferrables
   *     are waiting for {@link EmbeddingDeferredAction} we create Embedding Tasks for those.
   * @return The operation to run
   * @param <TaskT> The type of original task to run.
   */
  public static <TaskT extends Task<TableSchemaObject>>
      Operation<TableSchemaObject> createOperation(
          CommandContext<TableSchemaObject> commandContext,
          TaskGroupAndDeferrables<TaskT, TableSchemaObject> tasksAndDeferrables) {

    // Deferrables may or may not have DeferredValues and those DeferredValues may or maynot be
    // waiting for EmbeddingActions to be resolved.
    var embeddingActions =
        DeferredAction.filtered(
            EmbeddingDeferredAction.class, Deferrable.deferred(tasksAndDeferrables.deferrables()));

    if (embeddingActions.isEmpty()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "createOperation() - zero embeddingActions, creating direct TaskOperation operation tasksAndDeferrables.taskGroup().size()={}",
            tasksAndDeferrables.taskGroup().size());
      }
      // basic task, just wrap the tasks in an operation and go
      return new TaskOperation<>(
          tasksAndDeferrables.taskGroup(), tasksAndDeferrables.accumulator());
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "createOperation() - creating CompositeTask Operation, embeddingActions.size()={}, tasksAndDeferrables.taskGroup().size()={}",
          embeddingActions.size(),
          tasksAndDeferrables.taskGroup().size());
    }

    var compositeBuilder = new CompositeTaskOperationBuilder<>(commandContext);

    // Send the EmbeddingAction's to the builder to get back a list of EmbeddingTasks
    // that are linked to the actions they get vectors for.
    // th builder handles the grouping of the actions
    var embeddingTaskGroup =
        new EmbeddingTaskGroupBuilder<TableSchemaObject>()
            .withCommandContext(commandContext)
            .withEmbeddingActions(embeddingActions)
            .withRequestType(EmbeddingProvider.EmbeddingRequestType.SEARCH)
            .build();

    compositeBuilder.withIntermediateTasks(embeddingTaskGroup, TaskRetryPolicy.NO_RETRY);

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "createOperation() - created EmbeddingTasks embeddingTaskGroup.size={}",
          embeddingTaskGroup.size());
    }

    // we want to run a group of embedding tasks and then a group of the other tasks,
    // the two groups are linked by the EmbeddingAction objects
    // Because these are tables we only use the driver retry for the inserts, not task level retry
    return compositeBuilder.build(
        tasksAndDeferrables.taskGroup(),
        TaskRetryPolicy.NO_RETRY,
        tasksAndDeferrables.accumulator());
  }
}
