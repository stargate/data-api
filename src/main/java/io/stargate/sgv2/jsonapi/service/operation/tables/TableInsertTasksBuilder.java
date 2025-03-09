package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.InsertDBTask;
import io.stargate.sgv2.jsonapi.service.operation.InsertDBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.operation.tasks.*;
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
public class TableInsertTasksBuilder
    extends TaskBuilder<
        InsertDBTask<TableSchemaObject>, TableSchemaObject, TableInsertTasksBuilder> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableInsertTasksBuilder.class);

  private final CommandContext<TableSchemaObject> commandContext;
  private JsonNamedValueFactory jsonNamedValueFactory = null;
  private Boolean ordered = null;
  private Boolean returnDocumentResponses = null;

  public TableInsertTasksBuilder(CommandContext<TableSchemaObject> commandContext) {
    super(commandContext.schemaObject());
    this.commandContext = commandContext;
  }

  public TableInsertTasksBuilder withOrdered(Boolean ordered) {
    this.ordered = ordered;
    return this;
  }

  public TableInsertTasksBuilder withReturnDocumentResponses(Boolean returnDocumentResponses) {
    this.returnDocumentResponses = returnDocumentResponses;
    return this;
  }

  public TableInsertTasksBuilder withJsonNamedValueFactory(
      JsonNamedValueFactory jsonNamedValueFactory) {
    this.jsonNamedValueFactory = jsonNamedValueFactory;
    return this;
  }

  public TaskGroupAndDeferrables<InsertDBTask<TableSchemaObject>, TableSchemaObject> build(
      List<JsonNode> jsonNodes) {
    Objects.requireNonNull(jsonNodes, "jsonNodes cannot be null");
    Objects.requireNonNull(jsonNamedValueFactory, "jsonNamedValueFactory cannot be null");
    Objects.requireNonNull(ordered, "ordered cannot be null");
    Objects.requireNonNull(returnDocumentResponses, "returnDocumentResponses cannot be null");

    List<JsonNamedValueFactory.ParsedJsonDocument> parsedDocuments =
        jsonNamedValueFactory.create(jsonNodes);

    commandContext
        .requestTracing()
        .maybeTrace("Parsed Insert JSON Documents", Recordable.copyOf(parsedDocuments));

    var writeableTableRowBuilder =
        new WriteableTableRowBuilder(commandContext, JSONCodecRegistries.DEFAULT_REGISTRY);

    var tasksAndDeferrables =
        new TaskGroupAndDeferrables<>(
            new TaskGroup<>(ordered),
            InsertDBTaskPage.accumulator(commandContext)
                .returnDocumentResponses(returnDocumentResponses),
            new ArrayList<>());

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

      tasksAndDeferrables.taskGroup().add(task);
      if (writeableRow != null) {
        tasksAndDeferrables.deferrables().add(writeableRow);
      }
    }
    return tasksAndDeferrables;
  }
}
