package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.service.operation.InsertAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowShredder;
import io.stargate.sgv2.jsonapi.service.shredding.tables.WriteableTableRow;
import java.util.Objects;

/**
 * Builds a {@link TableInsertAttempt}.
 *
 * <p>NOTE: Uses the {@link RowShredder} and {@link WriteableTableRowBuilder} which both check the
 * data is valid, the first that the document does not exceed the limits, and the second that the
 * data is valid for the table.
 */
public class TableInsertAttemptBuilder implements InsertAttemptBuilder<TableInsertAttempt> {

  private final RowShredder rowShredder;
  private final WriteableTableRowBuilder writeableTableRowBuilder;
  // first value is zero, but we increment before we use it
  private int insertPosition = -1;

  public TableInsertAttemptBuilder(
      RowShredder rowShredder, WriteableTableRowBuilder writeableTableRowBuilder) {

    this.rowShredder = Objects.requireNonNull(rowShredder, "rowShredder cannot be null");
    this.writeableTableRowBuilder =
        Objects.requireNonNull(
            writeableTableRowBuilder, "writeableTableRowProvider cannot be null");
  }

  @Override
  public TableInsertAttempt build(JsonNode jsonNode) {

    WriteableTableRow writeableRow = null;
    Exception exception = null;
    try {
      var jsonContainer = rowShredder.shred(jsonNode);
      writeableRow = writeableTableRowBuilder.build(jsonContainer);
    } catch (Exception e) {
      exception = e;
    }

    insertPosition += 1;
    var rowId = writeableRow == null ? null : writeableRow.rowId();
    var attempt =
        new TableInsertAttempt(
            writeableTableRowBuilder.tableSchemaObject(), insertPosition, rowId, writeableRow);
    // ok to always add the failure, if it is null it will be ignored
    return (TableInsertAttempt) attempt.maybeAddFailure(exception);
  }
}
