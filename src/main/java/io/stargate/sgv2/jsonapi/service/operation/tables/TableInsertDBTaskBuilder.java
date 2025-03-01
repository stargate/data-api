package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.operation.InsertDBTask;
import io.stargate.sgv2.jsonapi.service.operation.InsertDBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingAction;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingTask;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.operation.tasks.*;
import io.stargate.sgv2.jsonapi.service.shredding.NamedValue;
import io.stargate.sgv2.jsonapi.service.shredding.tables.JsonNamedValueFactory;
import io.stargate.sgv2.jsonapi.service.shredding.tables.WriteableTableRow;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.*;
import java.util.stream.Collectors;
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

    var insertTasksAndRows = createInsertTasks(jsonNodes);

    // we have all the inserts, and the rows they are going to insert
    // if there are no deferred values in any of the rows we can return a single level task group
    // that just does
    // the inserts
    var allDeferredValues =
        insertTasksAndRows.stream()
            .map(insertTaskAndRow -> insertTaskAndRow.row().map(WriteableTableRow::deferredColumns))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .flatMap(Collection::stream)
            .toList();

    var insertTaskGroup =
        new TaskGroup<InsertDBTask<TableSchemaObject>, TableSchemaObject>(ordered);
    insertTasksAndRows.forEach(insertTaskAndRow -> insertTaskGroup.add(insertTaskAndRow.task()));

    TaskAccumulator<InsertDBTask<TableSchemaObject>, TableSchemaObject> insertAccumulator =
        InsertDBTaskPage.accumulator(commandContext)
            .returnDocumentResponses(returnDocumentResponses);

    if (allDeferredValues.isEmpty()) {
      return new TaskOperation<>(insertTaskGroup, insertAccumulator);
    }

    // we have some deferred values, e.g. we need to do vectorizing, so we need  to build a
    // hierarchy of task groups
    // for now we only have vectorize generators so quick sanity check
    var allActions = allDeferredValues.stream().map(NamedValue::valueAction).toList();
    var nonEmbeddingActions =
        allActions.stream().filter(action -> !(action instanceof EmbeddingAction)).toList();
    if (!nonEmbeddingActions.isEmpty()) {
      throw new RuntimeException("Unsupported value actions: " + nonEmbeddingActions);
    }
    var embeddingActions = allActions.stream().map(action -> (EmbeddingAction) action).toList();

    TaskGroup<EmbeddingTask<TableSchemaObject>, TableSchemaObject> embeddingTasks =
        createEmbeddingTasks(embeddingActions);

    // we now have an insert operation, with multiple insert tasks,
    // and an embedding operation with multiple embedding tasks
    // we need to make a group that can run these two tasks and an operation
    // that can accumulate the results

    // the composite group is sequential, we want to do the embedding first then the inserts.
    var compositeTaskGroup =
        new TaskGroup<CompositeTask<?, TableSchemaObject>, TableSchemaObject>(true);

    CompositeTask<EmbeddingTask<TableSchemaObject>, TableSchemaObject> embeddingCompositeTask =
        CompositeTask.<EmbeddingTask<TableSchemaObject>, TableSchemaObject>intermediaTask(
            nextPosition(), schemaObject, TaskRetryPolicy.NO_RETRY, embeddingTasks);

    compositeTaskGroup.add(embeddingCompositeTask);

    CompositeTask<InsertDBTask<TableSchemaObject>, TableSchemaObject> insertCompositeTask =
        CompositeTask.<InsertDBTask<TableSchemaObject>, TableSchemaObject>lastTask(
            nextPosition(),
            schemaObject,
            TaskRetryPolicy.NO_RETRY,
            insertTaskGroup,
            insertAccumulator);
    compositeTaskGroup.add(insertCompositeTask);

    // now we need an operation that will run the composite group
    CompositeTaskOuterPage.Accumulator<TableSchemaObject> outerAccumulator =
        CompositeTaskOuterPage.accumulator(commandContext);

    return new TaskOperation<>(compositeTaskGroup, outerAccumulator);
  }

  private List<InsertTaskAndRow> createInsertTasks(List<JsonNode> jsonNodes) {

    var writeableTableRowBuilder =
        new WriteableTableRowBuilder(commandContext, JSONCodecRegistries.DEFAULT_REGISTRY);

    List<JsonNamedValueFactory.ParsedJsonDocument> parsedDocuments =
        jsonNamedValueFactory.create(jsonNodes);

    commandContext
        .requestTracing()
        .maybeTrace("Parsed JSON Documents", Recordable.copyOf(parsedDocuments));

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

  //  private TaskOperation<InsertDBTask<TableSchemaObject>, TableSchemaObject>
  //      createInsertTaskOperation(List<InsertTaskAndRow> insertTaskAndRows) {
  //
  //    var taskGroup = new TaskGroup<InsertDBTask<TableSchemaObject>, TableSchemaObject>(ordered);
  //    insertTaskAndRows.forEach(insertTaskAndRow -> taskGroup.add(insertTaskAndRow.task()));
  //
  //    var accumulator =
  //        InsertDBTaskPage.accumulator(commandContext)
  //            .returnDocumentResponses(returnDocumentResponses);
  //
  //    return new TaskOperation<>(taskGroup, accumulator);
  //  }

  private TaskGroup<EmbeddingTask<TableSchemaObject>, TableSchemaObject> createEmbeddingTasks(
      List<EmbeddingAction> embeddingActions) {

    if (embeddingActions.isEmpty()) {
      throw new RuntimeException("No deferred vectorizing to do - TODO Handle better");
    }

    var actionGroups = groupByVectorizeDefinition(embeddingActions);

    // each group of embedding actions is a single Embedding Task
    TaskGroup<EmbeddingTask<TableSchemaObject>, TableSchemaObject> taskGroup =
        new TaskGroup<>(false);

    actionGroups.forEach(
        (embeddingActionGroupKey, actions) -> {
          var embeddingTask =
              EmbeddingTask.builder(commandContext)
                  .withApiVectorType(embeddingActionGroupKey.vectorType())
                  .withEmbeddingActions(actions)
                  .withRetryPolicy(TaskRetryPolicy.NO_RETRY)
                  .withOriginalCommandName(commandContext.commandName())
                  .withRequestType(EmbeddingProvider.EmbeddingRequestType.INDEX)
                  .build();
          taskGroup.add(embeddingTask);
        });
    return taskGroup;
  }

  //  private TaskOperation<EmbeddingTask<TableSchemaObject>, TableSchemaObject>
  //      createEmbeddingOperation(List<EmbeddingTask<TableSchemaObject>> embeddingTasks) {
  //
  //    var taskGroup = new TaskGroup<>(embeddingTasks);
  //
  //    var accumulator = EmbeddingTask.accumulator(commandContext);
  //
  //    return new TaskOperation<>(taskGroup, accumulator);
  //  }

  /**
   * We have embedding generation from potentially providers etc, we need to group all the deferred
   * values in to groups embeddingActions can share the same logically call (physically we may then
   * micro batch them)
   *
   * @param deferredEmbeddings
   * @return
   */
  private Map<EmbeddingAction.EmbeddingActionGroupKey, List<EmbeddingAction>>
      groupByVectorizeDefinition(List<EmbeddingAction> embeddingActions) {
    return embeddingActions.stream().collect(Collectors.groupingBy(EmbeddingAction::groupKey));
  }

  /**
   * The row can be null if there was an error making it, the task is still there to transport the
   * error for the row
   */
  private record InsertTaskAndRow(
      InsertDBTask<TableSchemaObject> task, Optional<WriteableTableRow> row) {}
}
