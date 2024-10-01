package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import io.stargate.sgv2.jsonapi.exception.ServerException;
import io.stargate.sgv2.jsonapi.exception.catchable.UnsupportedCqlTypeForDML;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.InsertAttempt;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.OrderedApiColumnDefContainer;
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

  /**
   * Override to describe the schema of the primary keys in the row we inserted
   *
   * @return
   */
  @Override
  public Optional<Object> schemaDescription() {

    var apiColumns = new OrderedApiColumnDefContainer(row.keyColumns().size());
    for (var cqlNamedValue : row.keyColumns().values()) {
      try {
        apiColumns.put(ApiColumnDef.from(cqlNamedValue.name()));
      } catch (UnsupportedCqlTypeForDML e) {
        throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
      }
    }
    return Optional.of(apiColumns);
  }
}
