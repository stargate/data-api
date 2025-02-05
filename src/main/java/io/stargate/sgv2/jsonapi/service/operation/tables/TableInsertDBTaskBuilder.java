package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.InsertDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowShredder;
import io.stargate.sgv2.jsonapi.service.shredding.tables.WriteableTableRow;
import java.util.Objects;

/**
 * Builds a {@link TableInsertDBTask}.
 *
 * <p>Create an instance and then call {@link #build(JsonNode)} for each task you want to create.
 *
 * <p>NOTE: Uses the {@link RowShredder} and {@link WriteableTableRowBuilder} which both check the
 * data is valid, the first that the document does not exceed the limits, and the second that the
 * data is valid for the table.
 */
public class TableInsertDBTaskBuilder extends TaskBuilder<InsertDBTask<TableSchemaObject>, TableSchemaObject> {

  private RowShredder rowShredder = null;
  private WriteableTableRowBuilder writeableTableRowBuilder = null;

  public TableInsertDBTaskBuilder(
      TableSchemaObject tableSchemaObject) {
    super(tableSchemaObject);
  }

  public TableInsertDBTaskBuilder withRowShredder(RowShredder rowShredder) {
    this.rowShredder =  Objects.requireNonNull(rowShredder, "rowShredder cannot be null");
    return this;
  }

  public TableInsertDBTaskBuilder withWriteableTableRowBuilder(WriteableTableRowBuilder writeableTableRowBuilder) {
    // we could build this in the builder, but keeping it as a prop for now incase it is easier for testing
    this.writeableTableRowBuilder = Objects.requireNonNull(writeableTableRowBuilder, "writeableTableRowBuilder cannot be null");
    return this;
  }

  public TableInsertDBTask build(JsonNode jsonNode) {
    Objects.requireNonNull(jsonNode, "jsonNode cannot be null");
    Objects.requireNonNull(rowShredder, "rowShredder cannot be null");

    WriteableTableRow writeableRow = null;
    Exception exception = null;
    try {
      var jsonContainer = rowShredder.shred(jsonNode);
      writeableRow = writeableTableRowBuilder.build(jsonContainer);
    } catch (RuntimeException e) {
      exception = e;
    }

    var rowId = writeableRow == null ? null : writeableRow.rowId();
    var task =
        new TableInsertDBTask(nextPosition(), schemaObject, getExceptionHandlerFactory(), rowId, writeableRow);
    // ok to always add the failure, if it is null it will be ignored
    return task.maybeAddFailure(exception);
  }
}
