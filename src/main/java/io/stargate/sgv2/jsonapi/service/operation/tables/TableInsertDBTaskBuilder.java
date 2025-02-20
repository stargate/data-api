package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.InsertDBTask;
import io.stargate.sgv2.jsonapi.service.operation.InsertDBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import io.stargate.sgv2.jsonapi.service.shredding.tables.JsonNamedValueFactory;
import io.stargate.sgv2.jsonapi.service.shredding.tables.WriteableTableRow;

import java.util.*;

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

    var tasksAndRows = createInsertTasks(jsonNodes);
    var insertTaskOperation = creqteInsertTaskOperation(tasksAndRows);

    // we have all the inserts, and the rows they are going to insert
    // if there are no deferred values in any of the rows we can return a single level task group
    // that just does
    // the inserts
    var allDeferredValues =
        tasksAndRows.stream()
            .map(taskAndRow -> taskAndRow.row().map(WriteableTableRow::deferredColumns))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .flatMap(Collection::stream)
            .toList();

    if (allDeferredValues.isEmpty()) {
      return insertTaskOperation;
    }
    throw new RuntimeException("Not implemented");
  }

  private List<TaskAndRow> createInsertTasks(List<JsonNode> jsonNodes) {

    var writeableTableRowBuilder =
        new WriteableTableRowBuilder(schemaObject, JSONCodecRegistries.DEFAULT_REGISTRY);

    List<TaskAndRow> taskAndRows = new ArrayList<>(jsonNodes.size());
    for (var jsonNode : jsonNodes) {

      WriteableTableRow writeableRow = null;
      Exception exception = null;
      // build the named values can result in errors like unknown columns or codec errors
      try {
        var jsonContainer = jsonNamedValueFactory.create(jsonNode);
        writeableRow = writeableTableRowBuilder.build(jsonContainer);
      } catch (RuntimeException e) {
        exception = e;
      }

      var rowId = writeableRow == null ? null : writeableRow.rowId();
      var task =
          new TableInsertDBTask(
              nextPosition(), schemaObject, getExceptionHandlerFactory(), rowId, writeableRow);
      // ok to always add the failure, if it is null it will be ignored
      task.maybeAddFailure(exception);

      taskAndRows.add(new TaskAndRow(task, Optional.ofNullable(writeableRow)));
    }
    return taskAndRows;
  }

  private TaskOperation<InsertDBTask<TableSchemaObject>, TableSchemaObject>
      creqteInsertTaskOperation(List<TaskAndRow> taskAndRows) {

    var taskGroup = new TaskGroup<InsertDBTask<TableSchemaObject>, TableSchemaObject>(ordered);
    taskAndRows.forEach(taskAndRow -> taskGroup.add(taskAndRow.task()));

    var accumulator =
        InsertDBTaskPage.accumulator(commandContext)
            .returnDocumentResponses(returnDocumentResponses);

    return new TaskOperation<>(taskGroup, accumulator);
  }

  /**
   * The row can be null if there was an error making it, the task is still there to transport the error for the row
   */
  private record TaskAndRow(InsertDBTask<TableSchemaObject> task, Optional<WriteableTableRow> row) {
  }
}
