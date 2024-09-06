package io.stargate.sgv2.jsonapi.service.shredding.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.exception.ServerException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistry;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.UnknownColumnException;
import io.stargate.sgv2.jsonapi.service.processor.SchemaValidatable;
import io.stargate.sgv2.jsonapi.service.shredding.DocRowIdentifer;
import io.stargate.sgv2.jsonapi.service.shredding.WritableDocRow;
import java.util.Map;
import java.util.Optional;

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
    checkUnsupportedColumnType(tableMetadata);
  }

  private boolean contains(CqlIdentifier column) {
    return allColumnValues().containsKey(column);
  }

  private boolean contains(ColumnMetadata column) {
    return contains(column.getName());
  }

  /** Checks if the row has all the primary key columns that are part of the table. */
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

  /** Checks if the row has any columns that are not part of the table. */
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

  /**
   * Checks if the row has any columns that are in the table but have a type that is not supported.
   *
   * <p>NOTE: AARON 3 aug 2024 - debatable if we do this here or in the operation, doing here
   * because we started adding detailed errors and tests, can be moved later
   *
   * <p>NOTE: CHECK FOR UNKNOWN COLUMNS MUST GO FIRST, this will raise server error if there is an
   * unknown colums
   *
   * @param tableMetadata
   */
  private void checkUnsupportedColumnType(TableMetadata tableMetadata) {

    var unsupportedMetadata =
        allColumnValues().entrySet().stream()
            .filter(
                entry -> {
                  try {
                    JSONCodecRegistry.codecToCQL(tableMetadata, entry.getKey(), entry.getValue());
                  } catch (UnknownColumnException e) {
                    throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
                  } catch (MissingJSONCodecException e) {
                    return true;
                  }
                  return false;
                })
            .map(entry -> tableMetadata.getColumn(entry.getKey()))
            .map(Optional::get) // we know it is present
            .toList();

    if (!unsupportedMetadata.isEmpty()) {
      throw DocumentException.Code.UNSUPPORTED_COLUMN_TYPES.get(
          Map.of(
              "keyspace", errFmt(tableMetadata.getKeyspace()),
              "table", errFmt(tableMetadata.getName()),
              "allColumns", errFmtColumnMetadata(tableMetadata.getColumns().values()),
              "unsupportedColumns", errFmtColumnMetadata(unsupportedMetadata)));
    }
  }
}
