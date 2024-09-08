package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.util.Strings;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.exception.ServerException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.*;
import io.stargate.sgv2.jsonapi.service.shredding.*;
import io.stargate.sgv2.jsonapi.service.shredding.collections.JsonPath;
import io.stargate.sgv2.jsonapi.service.shredding.tables.WriteableTableRow;


import java.util.*;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;

public class WriteableTableRowBuilder {


  private final TableSchemaObject tableSchemaObject;
  private final TableMetadata tableMetadata;
  private final JSONCodecRegistry codecRegistry;

  public WriteableTableRowBuilder(TableSchemaObject tableSchemaObject, JSONCodecRegistry codecRegistry) {
    this.tableSchemaObject = tableSchemaObject;
    this.tableMetadata = tableSchemaObject.tableMetadata;
    this.codecRegistry = codecRegistry;
  }

  public TableSchemaObject getTableSchemaObject() {
    return tableSchemaObject;
  }

  public WriteableTableRow build(JsonNamedValueContainer source) {

    // To be extra safe, create the CQLIdentifier for each field in the RawShreddedDocument and link it back
    // to the entry from the RawShreddedDocument, we will use this later when we convert the raw value with the codec
    // NOTE: this is where we DOUBLE QUOTE every raw field name to make sure it is a valid CQL identifier
    Map<CqlIdentifier, JsonNamedValue> cqlIdentifierToJsonValue = new HashMap<>();
    source.forEach((key, value) -> cqlIdentifierToJsonValue.put(createCqlIdentifier(key), value));

    checkAllPrimaryKeys(cqlIdentifierToJsonValue.keySet());
    checkUnknownColumns(cqlIdentifierToJsonValue.keySet());
    var decoded = decodeJsonToCQL(cqlIdentifierToJsonValue);

    // now need to split the columns into key and non key columns
    var keyColumns = new OrderedCqlNamedValueContainer();
    var nonKeyColumns = new UnorderedCqlNamedValueContainer();
    for (var cqlNamedValue : decoded.values()) {

      if (tableMetadata.getPrimaryKey().contains(cqlNamedValue.name())) {
        keyColumns.put(cqlNamedValue);
      } else {
        nonKeyColumns.put(cqlNamedValue);
      }
    }
    return new WriteableTableRow(tableSchemaObject, keyColumns, nonKeyColumns);
  }

  /**
   * Uses similar logic to the {@link CqlIdentifier#fromCql(String)} and double quotes the string if it is not
   * already quoted
   */
  private static CqlIdentifier createCqlIdentifier(JsonPath name) {
    if (Strings.isDoubleQuoted(name.toString())) {
      return CqlIdentifier.fromCql(name.toString());
    }
    return CqlIdentifier.fromCql(Strings.doubleQuote(name.toString()));
  }

  /** Checks if the row has all the primary key columns that are part of the table. */
  private void checkAllPrimaryKeys(Collection<CqlIdentifier> suppliedColumns) {

    // dont worry about set, there is normally only 1 to 3 primary key columns in a table
    var missingPrimaryKeys =
        tableMetadata.getPrimaryKey().stream()
            .filter(column -> !suppliedColumns.contains(column.getName()))
            .toList();

    if (!missingPrimaryKeys.isEmpty()) {
      var suppliedPrimaryKeys =
          tableMetadata.getPrimaryKey().stream()
              .filter(column -> suppliedColumns.contains(column.getName()))
              .toList();
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
  private void checkUnknownColumns(Collection<CqlIdentifier> suppliedColumns) {

    var unknownColumns =suppliedColumns.stream()
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
  private UnorderedCqlNamedValueContainer decodeJsonToCQL(Map<CqlIdentifier, JsonNamedValue> cqlIdentifierToRaw) {

    UnorderedCqlNamedValueContainer decoded = new UnorderedCqlNamedValueContainer();
    Map<ColumnMetadata, MissingJSONCodecException> unsupportedErrors = new HashMap<>();
    Map<ColumnMetadata, ToCQLCodecException> codecErrors = new HashMap<>();

    for (var entry : cqlIdentifierToRaw.entrySet()) {

      CqlIdentifier identifier = entry.getKey();
      JsonNamedValue jsonNamedValue = entry.getValue();
      var rawJsonValue = jsonNamedValue.value().value();

      ColumnMetadata metadata = tableMetadata.getColumn(identifier)
          .orElseThrow(() -> new IllegalStateException(
              String.format("decodeJsonToCQL: column not found in table metadata, column=%s", identifier)));

      try {
        var codec = codecRegistry.codecToCQL(tableMetadata, identifier, rawJsonValue);
        var cqlNamedValue = new CqlNamedValue(metadata, codec.toCQL(rawJsonValue));
        decoded.put(cqlNamedValue);
      } catch (UnknownColumnException e) {
        // this should not happen, we checked above but the codecs are written to be very safe and will check and throw
        throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
      } catch (MissingJSONCodecException e) {
        unsupportedErrors.put(metadata, e);
      }
      catch (ToCQLCodecException e) {
        codecErrors.put(metadata, e);
      }
    }

    // Check these first and throw, writing to types we don't support is more serious than sending out of range values.
    if (!unsupportedErrors.isEmpty()) {
      throw DocumentException.Code.UNSUPPORTED_COLUMN_TYPES.get(
          Map.of(
              "keyspace", errFmt(tableMetadata.getKeyspace()),
              "table", errFmt(tableMetadata.getName()),
              "allColumns", errFmtColumnMetadata(tableMetadata.getColumns().values()),
              "unsupportedColumns", errFmtColumnMetadata(unsupportedErrors.keySet())));
    }

    if (!codecErrors.isEmpty()) {
      var invalidColumns = errFmtJoin(codecErrors.entrySet(), entry -> {
        var metadata = entry.getKey();
        var error = entry.getValue();
        return String.format(
            "%s: %s",
            errFmt(metadata.getName()),
            error.getMessage());
      });
      throw DocumentException.Code.INVALID_COLUMN_VALUES.get(
          errVars(tableSchemaObject, map -> {
            map.put("allColumns", errFmtColumnMetadata(tableMetadata.getColumns().values()));
            map.put("invalidColumns", invalidColumns);
          })
      );
    }
    if (decoded.size() != cqlIdentifierToRaw.size()) {
      throw new IllegalStateException(
          String.format(
              "decodeJsonToCQL: decoded size does not match raw size, decoded=%d, raw=%d",
              decoded.size(), cqlIdentifierToRaw.size()));
    }
    return decoded;
  }

}
