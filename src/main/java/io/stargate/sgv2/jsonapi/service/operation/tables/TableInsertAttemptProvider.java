package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.service.operation.InsertAttemptProvider;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowShredder;
import io.stargate.sgv2.jsonapi.service.shredding.tables.WriteableTableRow;

import java.util.Objects;

public class TableInsertAttemptProvider implements InsertAttemptProvider<TableInsertAttempt> {

  private final RowShredder rowShredder;
  private final WriteableTableRowBuilder writeableTableRowBuilder;
  // first value is zero, but we increment before we use it
  private int insertPosition = -1;

  public TableInsertAttemptProvider(
      RowShredder rowShredder,
      WriteableTableRowBuilder writeableTableRowBuilder) {

    this.rowShredder = Objects.requireNonNull(rowShredder, "rowShredder cannot be null");
    this.writeableTableRowBuilder = Objects.requireNonNull(writeableTableRowBuilder, "writeableTableRowProvider cannot be null");
  }


  @Override
  public TableInsertAttempt apply(JsonNode jsonNode) {

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
    var attempt = new TableInsertAttempt(writeableTableRowBuilder.getTableSchemaObject(), insertPosition, rowId, writeableRow);
    // ok to always add the failure, if it is null it will be ignored
    return (TableInsertAttempt) attempt.maybeAddFailure(exception);

  }
}
