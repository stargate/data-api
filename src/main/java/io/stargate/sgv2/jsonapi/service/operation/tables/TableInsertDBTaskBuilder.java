package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.operation.InsertDBTask;
import io.stargate.sgv2.jsonapi.service.operation.InsertDBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingAction;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingTaskGroupBuilder;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.operation.tasks.*;
import io.stargate.sgv2.jsonapi.service.shredding.NamedValue;
import io.stargate.sgv2.jsonapi.service.shredding.tables.JsonNamedValueFactory;
import io.stargate.sgv2.jsonapi.service.shredding.tables.WriteableTableRow;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds a {@link TableInsertDBTask}.
 *
 * <p>Create an instance and then call {@link #build(JsonNode)} for each task you want to create.
 *
 * <p>NOTE: Uses the {@link JsonNamedValueFactory} and {@link WriteableTableRowBuilder} which both
 * check the data is valid, the first that the document does not exceed the limits, and the second
 * that the data is valid for the table.
 */
public class TableInsertDBTaskBuilder
    extends TaskBuilder<InsertDBTask<TableSchemaObject>, TableSchemaObject> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableInsertDBTaskBuilder.class);

  private final CommandContext<TableSchemaObject> commandContext;
  private JsonNamedValueFactory jsonNamedValueFactory = null;
  private Boolean ordered = null;
  private Boolean returnDocumentResponses = null;

  public TableInsertDBTaskBuilder(CommandContext<TableSchemaObject> commandContext) {
    super(commandContext.schemaObject());
    this.commandContext = commandContext;
  }

  public TableInsertDBTaskBuilder withOrdered(Boolean ordered) {
    this.ordered = ordered;
    return this;
  }

  public TableInsertDBTaskBuilder withReturnDocumentResponses(Boolean returnDocumentResponses) {
    this.returnDocumentResponses = returnDocumentResponses;
    return this;
  }

  public TableInsertDBTaskBuilder withJsonNamedValueFactory(
      JsonNamedValueFactory jsonNamedValueFactory) {
    this.jsonNamedValueFactory = jsonNamedValueFactory;
    return this;
  }

  public Operation<TableSchemaObject> build(List<JsonNode> jsonNodes) {
    Objects.requireNonNull(jsonNodes, "jsonNodes cannot be null");
    Objects.requireNonNull(jsonNamedValueFactory, "jsonNamedValueFactory cannot be null");
    Objects.requireNonNull(ordered, "ordered cannot be null");
    Objects.requireNonNull(returnDocumentResponses, "returnDocumentResponses cannot be null");

    // Setup the Inserts tasks, we know we always need these
    var insertTasksAndRows = createInsertTasks(jsonNodes);
    TaskGroup<InsertDBTask<TableSchemaObject>, TableSchemaObject> insertTaskGroup =
        new TaskGroup<>(ordered);
    insertTasksAndRows.forEach(insertTaskAndRow -> insertTaskGroup.add(insertTaskAndRow.task()));
    var insertAccumulator =
        InsertDBTaskPage.accumulator(commandContext)
            .returnDocumentResponses(returnDocumentResponses);

    // we have all the inserts, and the rows they are going to insert
    // if there are no deferred values in any of the rows we can return a single level task group
    // that just does the inserts
    var allDeferredValues =
        insertTasksAndRows.stream()
            .map(insertTaskAndRow -> insertTaskAndRow.row().map(WriteableTableRow::deferredColumns))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .flatMap(Collection::stream)
            .toList();

    if (allDeferredValues.isEmpty()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "build() - no deferred values, returning single level task group insertTaskGroup.size={}",
            insertTaskGroup.size());
      }
      return new TaskOperation<>(insertTaskGroup, insertAccumulator);
    }

    // we have some deferred values, e.g. we need to do vectorizing, so we need  to build a
    // hierarchy of task groups
    // for now we only have vectorize generators so quick sanity check
    var allActions = allDeferredValues.stream().map(NamedValue::valueAction).toList();
    var nonEmbeddingActions =
        allActions.stream().filter(action -> !(action instanceof EmbeddingAction)).toList();
    if (!nonEmbeddingActions.isEmpty()) {
      throw new IllegalArgumentException(
          "Unsupported actions in deferred values: " + nonEmbeddingActions);
    }

    // Send the EmbeddingAction's to the builder to get back a list of EmbeddingTasks
    // that are linked to the actions they get vectors for.
    var embeddingTaskGroup =
        new EmbeddingTaskGroupBuilder<TableSchemaObject>()
            .withCommandContext(commandContext)
            .withEmbeddingActions(
                allActions.stream().map(action -> (EmbeddingAction) action).toList())
            .withRequestType(EmbeddingProvider.EmbeddingRequestType.INDEX)
            .build();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "build() - deferred values for vectorizing, returning composite task group with embeddingTaskGroup.size={}, insertTaskGroup.size={}",
          embeddingTaskGroup.size(),
          insertTaskGroup.size());
    }

    // we want to run a group of embedding tasks and then a group of inserts,
    // the two groups are linked by the EmbeddingAction objects
    // Because these are tables we only use the driver retry for the inserts, not task level retry
    return new CompositeTaskOperationBuilder<>(commandContext)
        .withIntermediateTasks(embeddingTaskGroup, TaskRetryPolicy.NO_RETRY)
        .build(insertTaskGroup, TaskRetryPolicy.NO_RETRY, insertAccumulator);

    // the composite group is sequential, we want to do the embedding first then the inserts.
    //    var compositeTaskGroup =
    //        new TaskGroup<CompositeTask<?, TableSchemaObject>, TableSchemaObject>(true);
    //
    //    CompositeTask<EmbeddingTask<TableSchemaObject>, TableSchemaObject> embeddingCompositeTask
    // =
    //        CompositeTask.intermediateTask(
    //            nextPosition(), schemaObject, TaskRetryPolicy.NO_RETRY, embeddingTaskGroup);
    //
    //    compositeTaskGroup.add(embeddingCompositeTask);
    //
    //    CompositeTask<InsertDBTask<TableSchemaObject>, TableSchemaObject> insertCompositeTask =
    //        CompositeTask.lastTask(
    //            nextPosition(),
    //            schemaObject,
    //            TaskRetryPolicy.NO_RETRY,
    //            insertTaskGroup,
    //            insertAccumulator);
    //    compositeTaskGroup.add(insertCompositeTask);
    //
    //    // now we need an operation that will run the composite group
    //    CompositeTaskOuterPage.Accumulator<TableSchemaObject> outerAccumulator =
    //        CompositeTaskOuterPage.accumulator(commandContext);
    //
    //    return new TaskOperation<>(compositeTaskGroup, outerAccumulator);
  }

  private List<InsertTaskAndRow> createInsertTasks(List<JsonNode> jsonNodes) {

    List<JsonNamedValueFactory.ParsedJsonDocument> parsedDocuments =
        jsonNamedValueFactory.create(jsonNodes);

    commandContext
        .requestTracing()
        .maybeTrace("Parsed JSON Documents", Recordable.copyOf(parsedDocuments));

    var writeableTableRowBuilder =
        new WriteableTableRowBuilder(commandContext, JSONCodecRegistries.DEFAULT_REGISTRY);

    List<InsertTaskAndRow> insertTaskAndRows = new ArrayList<>(jsonNodes.size());
    for (var parsedDocument : parsedDocuments) {

      WriteableTableRow writeableRow = null;
      Exception exception = null;
      // build the named values can result in errors like unknown columns or codec errors
      try {
        writeableRow = writeableTableRowBuilder.build(parsedDocument.namedValues());
      } catch (RuntimeException e) {
        exception = e;
      }

      var rowId = writeableRow == null ? null : writeableRow.rowId();
      var task =
          new TableInsertDBTask(
              nextPosition(), schemaObject, getExceptionHandlerFactory(), rowId, writeableRow);
      // ok to always add the failure, if it is null it will be ignored
      task.maybeAddFailure(exception);

      insertTaskAndRows.add(new InsertTaskAndRow(task, Optional.ofNullable(writeableRow)));
    }

    return insertTaskAndRows;
  }

  //  private TaskGroup<EmbeddingTask<TableSchemaObject>, TableSchemaObject> createEmbeddingTasks(
  //      List<EmbeddingAction> embeddingActions) {
  //
  //    if (embeddingActions.isEmpty()) {
  //      throw new RuntimeException("No deferred vectorizing to do - TODO Handle better");
  //    }
  //
  //    Map<EmbeddingAction.EmbeddingActionGroupKey, List<EmbeddingAction>> actionGroups =
  // embeddingActions.stream().collect(Collectors.groupingBy(EmbeddingAction::groupKey));
  //
  //    // each group of embedding actions is a single Embedding Task
  //    TaskGroup<EmbeddingTask<TableSchemaObject>, TableSchemaObject> taskGroup =
  //        new TaskGroup<>(false);
  //
  //    actionGroups.forEach(
  //        (embeddingActionGroupKey, actions) -> {
  //          var embeddingTask =
  //              EmbeddingTask.builder(commandContext)
  //                  .withApiVectorType(embeddingActionGroupKey.vectorType())
  //                  .withEmbeddingActions(actions)
  //                  .withRetryPolicy(TaskRetryPolicy.NO_RETRY)
  //                  .withOriginalCommandName(commandContext.commandName())
  //                  .withRequestType(EmbeddingProvider.EmbeddingRequestType.INDEX)
  //                  .build();
  //          taskGroup.add(embeddingTask);
  //        });
  //    return taskGroup;
  //  }

  /**
   * The row can be null if there was an error making it, the task is still there to transport the
   * error for the row
   */
  private record InsertTaskAndRow(
      InsertDBTask<TableSchemaObject> task, Optional<WriteableTableRow> row) {}
}
