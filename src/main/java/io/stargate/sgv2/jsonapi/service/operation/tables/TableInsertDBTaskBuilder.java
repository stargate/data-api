package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.operation.InsertDBTask;
import io.stargate.sgv2.jsonapi.service.operation.InsertDBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingAction;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingTaskGroupBuilder;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.operation.tasks.*;
import io.stargate.sgv2.jsonapi.service.shredding.Deferrable;
import io.stargate.sgv2.jsonapi.service.shredding.ValueAction;
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
    extends TaskBuilder<
        InsertDBTask<TableSchemaObject>, TableSchemaObject, TableInsertDBTaskBuilder> {

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
    insertTaskGroup.addAll(insertTasksAndRows.tasks());
    var insertAccumulator =
        InsertDBTaskPage.accumulator(commandContext)
            .returnDocumentResponses(returnDocumentResponses);

    // we have all the inserts, and the rows they are going to insert
    // if there are no deferred values in any of the rows we can return a single level task group
    // that just does the inserts
    var allDeferredValues = Deferrable.deferredValues(insertTasksAndRows.rows());

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
    // for now we only have vectorize actions so quick sanity check

    var allActions = ValueAction.filteredActions(ValueAction.class, allDeferredValues);
    var embeddingActions = ValueAction.filteredActions(EmbeddingAction.class, allDeferredValues);
    if (allActions.size() != embeddingActions.size()) {
      throw new IllegalArgumentException("Unsupported actions in deferred values: " + allActions);
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
  }

  private TasksAndRows createInsertTasks(List<JsonNode> jsonNodes) {

    List<JsonNamedValueFactory.ParsedJsonDocument> parsedDocuments =
        jsonNamedValueFactory.create(jsonNodes);

    commandContext
        .requestTracing()
        .maybeTrace("Parsed JSON Documents", Recordable.copyOf(parsedDocuments));

    var writeableTableRowBuilder =
        new WriteableTableRowBuilder(commandContext, JSONCodecRegistries.DEFAULT_REGISTRY);

    TasksAndRows tasksAndRows = new TasksAndRows(jsonNodes.size());
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

      tasksAndRows.tasks.add(task);
      if (writeableRow != null) {
        tasksAndRows.rows.add(writeableRow);
      }
    }

    return tasksAndRows;
  }

  /**
   * The row can be null if there was an error making it, the task is still there to transport the
   * error for the row.
   *
   * <p>We are going to test for the rows for deferrable values, so put them on that interface now.
   */
  private record TasksAndRows(List<InsertDBTask<TableSchemaObject>> tasks, List<Deferrable> rows) {
    TasksAndRows(int initialCapacity) {
      this(new ArrayList<>(initialCapacity), new ArrayList<>(initialCapacity));
    }
  }
}
