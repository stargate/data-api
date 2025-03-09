package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.COLUMN_METADATA_COMPARATOR;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.*;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiSupportDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTableDef;
import io.stargate.sgv2.jsonapi.service.shredding.*;
import io.stargate.sgv2.jsonapi.service.shredding.tables.CqlNamedValueFactory;
import io.stargate.sgv2.jsonapi.service.shredding.tables.WriteableTableRow;
import java.util.*;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds a {@link WriteableTableRow} from a {@link JsonNamedValueContainer}.
 *
 * <p>This class does all the checks that the columns and data we are trying to insert are valid, it
 * enforces the rules that {@link WriteableTableRow} has to be valid.
 */
public class WriteableTableRowBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(WriteableTableRowBuilder.class);

  /** Match if a column does not support insert. */
  private static final Predicate<ApiSupportDef> MATCH_INSERT_UNSUPPORTED =
      ApiSupportDef.Matcher.NO_MATCHES.withInsert(false);

  public static final CqlNamedValue.ErrorStrategy<DocumentException> ERROR_STRATEGY =
      new CqlNamedValue.ErrorStrategy<>() {

        @Override
        public void allChecks(
            TableSchemaObject tableSchemaObject, CqlNamedValueContainer allColumns) {
          checkUnknownColumns(tableSchemaObject, allColumns);
          checkApiSupport(tableSchemaObject, allColumns, MATCH_INSERT_UNSUPPORTED);
          checkMissingCodec(tableSchemaObject, allColumns);
          checkCodecError(tableSchemaObject, allColumns);
          checkMissingVectorize(tableSchemaObject, allColumns);
        }

        @Override
        public ErrorCode<DocumentException> codeForNoApiSupport() {
          return DocumentException.Code.UNSUPPORTED_COLUMN_TYPES;
        }

        @Override
        public ErrorCode<DocumentException> codeForUnknownColumn() {
          return DocumentException.Code.UNKNOWN_TABLE_COLUMNS;
        }

        @Override
        public ErrorCode<DocumentException> codeForMissingVectorize() {
          return DocumentException.Code.UNSUPPORTED_VECTORIZE_WHEN_MISSING_VECTORIZE_DEFINITION;
        }

        @Override
        public ErrorCode<DocumentException> codeForMissingCodec() {
          return DocumentException.Code.UNSUPPORTED_COLUMN_TYPES;
        }

        @Override
        public ErrorCode<DocumentException> codeForCodecError() {
          return DocumentException.Code.INVALID_COLUMN_VALUES;
        }
      };

  private final CommandContext<TableSchemaObject> commandContext;
  private final TableSchemaObject tableSchemaObject;
  private final JSONCodecRegistry codecRegistry;

  private final ApiTableDef apiTableDef;
  private final TableMetadata tableMetadata;

  public WriteableTableRowBuilder(
      CommandContext<TableSchemaObject> commandContext, JSONCodecRegistry codecRegistry) {

    this.commandContext = Objects.requireNonNull(commandContext, "commandContext cannot be null");
    this.tableSchemaObject = commandContext.schemaObject();
    this.codecRegistry = Objects.requireNonNull(codecRegistry, "codecRegistry cannot be null");

    apiTableDef = tableSchemaObject.apiTableDef();
    // TOO: eventually only use the APi metadata not the driver
    tableMetadata = tableSchemaObject.tableMetadata();
  }

  /**
   * Builds a {@link WriteableTableRow} from a {@link JsonNamedValueContainer}, validating schema
   * and data using the state of the builder.
   *
   * @param source the {@link JsonNamedValueContainer} to build the {@link WriteableTableRow} from.
   * @return A valid {@link WriteableTableRow} that can be inserted.
   */
  public WriteableTableRow build(JsonNamedValueContainer source) {

    // Map everything from the JSON source into a CQL Value, we can check their state after.
    // the checks on the error strategy will run, we have some extra ones below

    var allColumns =
        new CqlNamedValueFactory(tableSchemaObject, codecRegistry, ERROR_STRATEGY).create(source);

    // TODO: move this check all primary keys into the error strategy
    checkAllPrimaryKeys(allColumns);

    // now need to split the columns into key and non-key columns
    var keyColumns = new CqlNamedValueContainer();
    var nonKeyColumns = new CqlNamedValueContainer();

    // Get the primary keys out of the new values in the order they are defined on the table
    for (var columnDef : apiTableDef.primaryKeys().values()) {
      if (allColumns.containsKey(columnDef.name())) {
        keyColumns.put(allColumns.get(columnDef.name()));
      } else {
        // the primary keys have been checked above
        throw new IllegalStateException(
            String.format(
                "build: primary key column not found in decoded values, column=%s", columnDef));
      }
    }

    // any column in decoded that is now not in keyColumns is a non key column
    for (var cqlNamedValue : allColumns.values()) {
      if (!keyColumns.containsKey(cqlNamedValue.name())) {
        nonKeyColumns.put(cqlNamedValue);
      }
    }

    return new WriteableTableRow(tableSchemaObject, keyColumns, nonKeyColumns);
  }

  //  public WriteableTableRow build(JsonNamedValueContainer source) {
  //
  //    // To be extra safe, create the CQLIdentifier for each JsonNamedValue and link it back
  //    // to the entry from the container, we will use this later when we convert the JSON value
  // with
  //    // the codec
  //    // NOTE: this is where we DOUBLE QUOTE every raw field name to make sure it is a valid CQL
  //    // identifier - see cqlIdentifierToJsonValue()
  //    Map<CqlIdentifier, JsonNamedValue> cqlIdentifierToJsonValue = new HashMap<>();
  //    source.forEach((key, value) -> cqlIdentifierToJsonValue.put(createCqlIdentifier(key),
  // value));
  //
  //    // the validation steps
  //    checkAllPrimaryKeys(cqlIdentifierToJsonValue);
  //    checkUnknownColumns(cqlIdentifierToJsonValue.keySet());
  //    checkApiSupport(cqlIdentifierToJsonValue.keySet());
  //    var decoded = encodeJsonToCql(cqlIdentifierToJsonValue);
  //
  //    // now need to split the columns into key and non key columns
  //    var keyColumns = new CqlNamedValueContainer();
  //    var nonKeyColumns = new CqlNamedValueContainer();
  //
  //    // Get the primary keys out of the new values in the order they are defined on the table
  //    for (var keyMetadata : tableMetadata.getPrimaryKey()) {
  //      if (decoded.containsKey(keyMetadata)) {
  //        keyColumns.put(decoded.get(keyMetadata));
  //      } else {
  //        // the primary keys have been checked above
  //        throw new IllegalStateException(
  //            String.format(
  //                "build: primary key column not found in decoded values, column=%s",
  // keyMetadata));
  //      }
  //    }
  //
  //    // any column in decoded that is now not in keyColumns is a non key column
  //    for (var cqlNamedValue : decoded.values()) {
  //      if (!keyColumns.containsKey(cqlNamedValue.name())) {
  //        nonKeyColumns.put(cqlNamedValue);
  //      }
  //    }
  //
  //    return new WriteableTableRow(tableSchemaObject, keyColumns, nonKeyColumns);
  //  }

  /**
   * Checks if the row has all the primary key columns that are part of the table, and the values
   * are not null
   *
   * <p><b>NOTE:</b> must check for codec errors before this, so we know we have a valid value to
   * check
   *
   * <p>Throws a {@link DocumentException.Code#MISSING_PRIMARY_KEY_COLUMNS}
   */
  private void checkAllPrimaryKeys(CqlNamedValueContainer allColumns) {

    // dont worry about set, there is normally only 1 to 3 primary key columns in a table

    Predicate<ColumnMetadata> isMissingPredicate =
        columMetadata -> {
          var cqlNamedValue = allColumns.get(columMetadata.getName());
          // missing is not present or present and null
          return cqlNamedValue == null || cqlNamedValue.value() == null;
        };

    var missingPrimaryKeys =
        tableMetadata.getPrimaryKey().stream()
            .filter(isMissingPredicate)
            .sorted(COLUMN_METADATA_COMPARATOR)
            .toList();

    if (!missingPrimaryKeys.isEmpty()) {
      var suppliedPrimaryKeys =
          tableMetadata.getPrimaryKey().stream().filter(isMissingPredicate.negate()).toList();

      throw DocumentException.Code.MISSING_PRIMARY_KEY_COLUMNS.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("primaryKeys", errFmtColumnMetadata(tableMetadata.getPrimaryKey()));
                map.put("providedKeys", errFmtColumnMetadata(suppliedPrimaryKeys));
                map.put("missingKeys", errFmtColumnMetadata(missingPrimaryKeys));
              }));
    }
  }

  /**
   * Checks all columns were bound to the schema.
   *
   * <p>Throws a {@link DocumentException.Code#UNKNOWN_TABLE_COLUMNS}
   */
  //  private void checkUnknownColumns(CqlNamedValueContainer allColumns) {
  //
  //    var unknownColumns =
  //        allColumns.values().stream()
  //            .filter(cqlNamedValue ->
  // cqlNamedValue.state().equals(NamedValue.NamedValueState.BIND_ERROR))
  //            .filter(cqlNamedValue ->
  // cqlNamedValue.errorCode().equals(INSERT_ERROR_CODES.unknownColumn()))
  //            .map(CqlNamedValue::name)
  //            .sorted(CqlIdentifierUtil.CQL_IDENTIFIER_COMPARATOR)
  //            .toList();
  //
  //    if (!unknownColumns.isEmpty()) {
  //      throw INSERT_ERROR_CODES.unknownColumn().get(
  //          errVars(
  //              tableSchemaObject,
  //              map -> {
  //                map.put("allColumns",
  // errFmtColumnMetadata(tableMetadata.getColumns().values()));
  //                map.put("unknownColumns", errFmtCqlIdentifier(unknownColumns));
  //              }));
  //    }
  //  }

  /**
   * Checks if the row has any columns that we do not support writing to.
   *
   * <p>While we also can hit a problem with not having a codec, we check using the {@link
   * io.stargate.sgv2.jsonapi.service.schema.tables.ApiSupportDef} because some types are read only
   * and so they have a codec entry.
   */
  //  private void checkApiSupport(CqlNamedValueContainer allColumns) {
  //
  //    var unsupportedColumns =
  //        tableSchemaObject
  //            .apiTableDef()
  //            .allColumns()
  //            .filterBy(allColumns.keySet())
  //            .filterBySupportToList(MATCH_INSERT_UNSUPPORTED);
  //
  //    if (!unsupportedColumns.isEmpty()) {
  //      // list is immutable
  //      var sortedUnsupportedColumns = new ArrayList<>(unsupportedColumns);
  //      sortedUnsupportedColumns.sort(ApiColumnDef.NAME_COMPARATOR);
  //
  //      // NOTE: SAME ERROR IS THROWN  in encodeJsonToCql -OK until we re-factor
  //      throw DocumentException.Code.UNSUPPORTED_COLUMN_TYPES.get(
  //          errVars(
  //              tableSchemaObject,
  //              map -> {
  //                map.put("allColumns",
  // errFmtColumnMetadata(tableMetadata.getColumns().values()));
  //                map.put("unsupportedColumns", errFmtApiColumnDef(sortedUnsupportedColumns));
  //              }));
  //    }
  //  }

  //  private void checkMissingCodec(CqlNamedValueContainer allColumns) {
  //
  //    var missingCodecs =
  //        allColumns.values().stream()
  //            .filter(cqlNamedValue ->
  // cqlNamedValue.state().equals(NamedValue.NamedValueState.PREPARE_ERROR))
  //            .filter(cqlNamedValue ->
  // cqlNamedValue.errorCode().equals(ERROR_STRATEGY.missingCodec()))
  //            .map(CqlNamedValue::name)
  //            .sorted(CqlIdentifierUtil.CQL_IDENTIFIER_COMPARATOR)
  //            .toList();
  //
  //    if (!missingCodecs.isEmpty()) {
  //      // NOTE: SAME ERROR IS THROWN  in checkApiSupport -OK until we re-factor
  //      throw ERROR_STRATEGY.missingCodec().get(
  //          errVars(
  //              tableSchemaObject,
  //              map -> {
  //                map.put("allColumns",
  // errFmtColumnMetadata(tableMetadata.getColumns().values()));
  //                map.put("unsupportedColumns", errFmtCqlIdentifier(missingCodecs));
  //              }));
  //    }
  //  }

  //  private void checkCodecError(CqlNamedValueContainer allColumns) {
  //
  //    var codecErrors =
  //        allColumns.values().stream()
  //            .filter(cqlNamedValue ->
  // cqlNamedValue.state().equals(NamedValue.NamedValueState.PREPARE_ERROR))
  //            .filter(cqlNamedValue ->
  // cqlNamedValue.errorCode().equals(ERROR_STRATEGY.codecError()))
  //            .map(CqlNamedValue::name)
  //            .sorted(CqlIdentifierUtil.CQL_IDENTIFIER_COMPARATOR)
  //            .toList();
  //
  //    if (!codecErrors.isEmpty()) {
  //      throw ERROR_STRATEGY.codecError().get(
  //          errVars(
  //              tableSchemaObject,
  //              map -> {
  //                map.put("allColumns",
  // errFmtColumnMetadata(tableMetadata.getColumns().values()));
  //                map.put("invalidColumns", errFmtCqlIdentifier(codecErrors));
  //              }));
  //    }}

  /**
   * Converts the raw JSON values to CQL values using the codec registry.
   *
   * <p>NOTE: CHECK FOR UNKNOWN COLUMNS MUST GO FIRST, this will raise server error if there is an
   * unknown columns
   *
   * <p>Throws a {@link DocumentException.Code#UNSUPPORTED_COLUMN_TYPES} if there are any columns
   * that have a type we do not have a codec for, if none of those will then throw a {@link
   * DocumentException.Code#INVALID_COLUMN_VALUES} if any of the values failed to convert to CQL
   * value.
   */
  //  private CqlNamedValueContainer encodeJsonToCql(
  //      Map<CqlIdentifier, JsonNamedValue> cqlIdentifierToRaw) {
  //
  //    var cqlNamedValues = new CqlNamedValueContainer();
  //    Map<ColumnMetadata, MissingJSONCodecException> unsupportedErrors = new HashMap<>();
  //    Map<ColumnMetadata, ToCQLCodecException> codecErrors = new HashMap<>();
  //
  //    for (var entry : cqlIdentifierToRaw.entrySet()) {
  //
  //      CqlIdentifier identifier = entry.getKey();
  //      JsonNamedValue jsonNamedValue = entry.getValue();
  //      var rawJsonValue = jsonNamedValue.value().value();
  //
  //      // Should not happen if checkUnknownColumns() is called first
  //      ColumnMetadata metadata =
  //          tableMetadata
  //              .getColumn(identifier)
  //              .orElseThrow(
  //                  () ->
  //                      new IllegalStateException(
  //                          String.format(
  //                              "decodeJsonToCQL: column not found in table metadata, column=%s",
  //                              identifier)));
  //
  //      try {
  //        var codec = codecRegistry.codecToCQL(tableMetadata, identifier, rawJsonValue);
  //        var cqlNamedValue = new CqlNamedValue(metadata, codec.toCQL(rawJsonValue));
  //        cqlNamedValues.put(cqlNamedValue);
  //      } catch (UnknownColumnException e) {
  //        // this should not happen, we checked above but the codecs are written to be very safe
  // and
  //        // will check and throw
  //        throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
  //      } catch (MissingJSONCodecException e) {
  //        unsupportedErrors.put(metadata, e);
  //      } catch (ToCQLCodecException e) {
  //        codecErrors.put(metadata, e);
  //      }
  //    }
  //
  //    // Check these first and throw, writing to types we don't support is more serious than
  // sending
  //    // out of range values.
  //    if (!unsupportedErrors.isEmpty()) {
  //      // NOTE: SAME ERROR IS THROWN  in checkApiSupport -OK until we re-factor
  //      throw DocumentException.Code.UNSUPPORTED_COLUMN_TYPES.get(
  //          errVars(
  //              tableSchemaObject,
  //              map -> {
  //                map.put("allColumns",
  // errFmtColumnMetadata(tableMetadata.getColumns().values()));
  //                map.put("unsupportedColumns", errFmtColumnMetadata(unsupportedErrors.keySet()));
  //              }));
  //    }
  //
  //    if (!codecErrors.isEmpty()) {
  //      // a string list of the columns and the codec error they generated
  //      var invalidColumns =
  //          errFmtJoin(
  //              codecErrors.entrySet(),
  //              entry -> {
  //                var metadata = entry.getKey();
  //                var error = entry.getValue();
  //                return String.format("%s: %s", errFmt(metadata.getName()), error.getMessage());
  //              });
  //      throw DocumentException.Code.INVALID_COLUMN_VALUES.get(
  //          errVars(
  //              tableSchemaObject,
  //              map -> {
  //                map.put("allColumns",
  // errFmtColumnMetadata(tableMetadata.getColumns().values()));
  //                map.put("invalidColumns", invalidColumns);
  //              }));
  //    }
  //
  //    if (cqlNamedValues.size() != cqlIdentifierToRaw.size()) {
  //      throw new IllegalStateException(
  //          String.format(
  //              "decodeJsonToCQL: decoded size does not match raw size, decoded=%d, raw=%d",
  //              cqlNamedValues.size(), cqlIdentifierToRaw.size()));
  //    }
  //    return cqlNamedValues;
  //  }

}
