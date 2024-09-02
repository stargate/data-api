package io.stargate.sgv2.jsonapi.service.shredding.tables;

import static io.stargate.sgv2.jsonapi.exception.playing.ErrorFormatters.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import io.stargate.sgv2.jsonapi.exception.playing.DocumentException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.processor.SchemaValidatable;
import io.stargate.sgv2.jsonapi.service.shredding.DocRowIdentifer;
import io.stargate.sgv2.jsonapi.service.shredding.WritableDocRow;
import java.util.Map;

/**
 * The data extracted from a JSON document that can be written to a table row.
 *
 * <p>Created by the {@link RowShredder}
 *
 * @param id
 * @param allColumnValues
 */
public record WriteableTableRow(RowId id, Map<CqlIdentifier, Object> allColumnValues)
    implements WritableDocRow, SchemaValidatable {

  @Override
  public DocRowIdentifer docRowID() {
    return id();
  }

  @Override
  /**
   * Validates the columns are included in the row are part of the table we are going to write to.
   *
   * <p>Happens here because the shredder is responsible for extracting what we consider columns and
   * values from the document.
   */
  public void validate(TableSchemaObject table) {

    var tableMetadata = table.tableMetadata;

    checkAllPrimaryKeys(tableMetadata);
    checkUnknownColumns(tableMetadata);
  }

  private boolean contains(CqlIdentifier column) {
    return allColumnValues().containsKey(column);
  }

  private boolean contains(ColumnMetadata column) {
    return contains(column.getName());
  }

  private void checkAllPrimaryKeys(TableMetadata tableMetadata) {

    var missingPrimaryKeys =
        tableMetadata.getPrimaryKey().stream().filter(column -> !contains(column)).toList();

    if (!missingPrimaryKeys.isEmpty()) {
      var suppliedPrimaryKeys =
          tableMetadata.getPrimaryKey().stream().filter(this::contains).toList();
      throw DocumentException.Code.MISSING_PRIMARY_KEY_COLUMNS.get(
          Map.of(
              "keyspace", errFmt(tableMetadata.getKeyspace()),
              "table", errFmt(tableMetadata.getName()),
              "primaryKeys", errFmtColumnMetadata(tableMetadata.getPrimaryKey()),
              "providedKeys", errFmtColumnMetadata(suppliedPrimaryKeys),
              "missingKeys", errFmtColumnMetadata(missingPrimaryKeys)));
    }
  }

  private void checkUnknownColumns(TableMetadata tableMetadata) {
    var unknownColumns =
        allColumnValues().keySet().stream()
            .filter(column -> tableMetadata.getColumn(column).isEmpty())
            .toList();

    if (!unknownColumns.isEmpty()) {
      throw DocumentException.Code.UNKNOWN_TABLE_COLUMNS.get(
          Map.of(
              "keyspace", errFmt(tableMetadata.getKeyspace()),
              "table", errFmt(tableMetadata.getName()),
              "allColumns", errFmtColumnMetadata(tableMetadata.getColumns().values()),
              "unknownColumns", errFmtCqlIdentifier(unknownColumns)));
    }
  }
}
