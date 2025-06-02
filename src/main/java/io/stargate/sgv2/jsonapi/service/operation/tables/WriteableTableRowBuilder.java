package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.COLUMN_METADATA_COMPARATOR;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.*;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTableDef;
import io.stargate.sgv2.jsonapi.service.shredding.*;
import io.stargate.sgv2.jsonapi.service.shredding.tables.CqlNamedValueContainerFactory;
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

  private static final CqlNamedValue.ErrorStrategy<DocumentException> ERROR_STRATEGY =
      new CqlNamedValue.ErrorStrategy<>() {

        @Override
        public void allChecks(
            TableSchemaObject tableSchemaObject, CqlNamedValueContainer allColumns) {
          checkUnknownColumns(tableSchemaObject, allColumns);
          checkApiSupport(tableSchemaObject, allColumns);
          checkMissingCodec(tableSchemaObject, allColumns);
          checkCodecError(tableSchemaObject, allColumns);
          checkMissingVectorize(tableSchemaObject, allColumns);
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

        private void checkApiSupport(
            TableSchemaObject tableSchemaObject, CqlNamedValueContainer allColumns) {

          var unsupportedColumns =
              allColumns.values().stream()
                  .filter(namedValue -> !namedValue.apiColumnDef().type().apiSupport().insert())
                  .sorted(CqlNamedValue.NAME_COMPARATOR)
                  .toList();

          if (!unsupportedColumns.isEmpty()) {
            throw DocumentException.Code.UNSUPPORTED_COLUMN_TYPES.get(
                errVars(
                    tableSchemaObject,
                    map -> {
                      map.put(
                          "allColumns",
                          errFmtColumnMetadata(
                              tableSchemaObject.tableMetadata().getColumns().values()));
                      map.put("unsupportedColumns", errFmtCqlNamedValue(unsupportedColumns));
                    }));
          }
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
        new CqlNamedValueContainerFactory(tableSchemaObject, codecRegistry, ERROR_STRATEGY)
            .create(source);

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
}
