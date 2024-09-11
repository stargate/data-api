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
import java.util.Objects;
import java.util.Optional;

public class TableInsertAttempt implements InsertAttempt {

  private final TableSchemaObject tableSchemaObject;
  private final int position;
  private final RowId rowId;
  private final WriteableTableRow row;
  private Throwable failure;

  TableInsertAttempt(
      TableSchemaObject tableSchemaObject, int position, RowId rowId, WriteableTableRow row) {
    this.tableSchemaObject =
        Objects.requireNonNull(tableSchemaObject, "tableSchemaObject cannot be null");
    this.position = position;
    this.rowId = rowId;
    this.row = row;
  }

  public TableInsertValuesCQLClause getInsertValuesCQLClause() {
    return new TableInsertValuesCQLClause(tableSchemaObject, row);
  }

  public Optional<WriteableTableRow> row() {
    return Optional.ofNullable(row);
  }

  @Override
  public int position() {
    return position;
  }

  @Override
  public Optional<DocRowIdentifer> docRowID() {
    return Optional.ofNullable(rowId);
  }

  @Override
  public Optional<Throwable> failure() {
    return Optional.ofNullable(failure);
  }

  @Override
  public InsertAttempt maybeAddFailure(Throwable failure) {
    if (this.failure == null) {
      this.failure = failure;
    }
    return this;
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
