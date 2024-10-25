package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnsDef;
import io.stargate.sgv2.jsonapi.exception.ServerException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlColumn;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.InsertAttempt;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDefContainer;
import io.stargate.sgv2.jsonapi.service.shredding.DocRowIdentifer;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowId;
import io.stargate.sgv2.jsonapi.service.shredding.tables.WriteableTableRow;
import java.util.Optional;

/**
 * An attempt to insert into an API Table, overrides the {@link InsertAttempt} to provide the row id
 * and schema.
 */
public class TableInsertAttempt extends InsertAttempt<TableSchemaObject> {

  private final RowId rowId;
  private final WriteableTableRow row;

  TableInsertAttempt(
      TableSchemaObject tableSchemaObject, int position, RowId rowId, WriteableTableRow row) {
    super(
        position,
        tableSchemaObject,
        row == null ? null : new TableInsertValuesCQLClause(tableSchemaObject, row));

    this.rowId = rowId;
    this.row = row;
    setStatus(OperationStatus.READY);
  }

  @Override
  public Optional<DocRowIdentifer> docRowID() {
    return Optional.ofNullable(rowId);
  }

  /** Override to describe the schema of the primary keys in the row we inserted */
  @Override
  public Optional<ColumnsDef> schemaDescription() {

    /// we could be in an error state, and not have inserted anything , if we got a shredding error
    // then we do not have the row to describe
    if (row == null) {
      return Optional.empty();
    }

    var apiColumns = new ApiColumnDefContainer(row.keyColumns().size());
    for (var cqlNamedValue : row.keyColumns().values()) {
      try {
        apiColumns.put(ApiColumnDef.from(cqlNamedValue.name()));
      } catch (UnsupportedCqlColumn e) {
        throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
      }
    }
    return Optional.of(apiColumns.toColumnsDef());
  }
}
