package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.NativeTypeTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilter;

import java.util.*;
import java.util.function.Consumer;

/**
 * The purpose for this analyzer class is to analyze the whereCQLClause.
 *
 * <p>There are two kinds of rules to check:<br>
 * 1. Validation check, gather all FilterException and terminate the analyze flow. 2. Warning check,
 * if the filter is valid, then do the warning check, gather all WarningException
 */
public class WhereCQLClauseAnalyzer {

  private final TableSchemaObject tableSchemaObject;
  private final TableMetadata tableMetadata;

  public WhereCQLClauseAnalyzer(TableSchemaObject tableSchemaObject) {
    this.tableSchemaObject = Objects.requireNonNull(tableSchemaObject, "tableSchemaObject cannot be null");
    this.tableMetadata = tableSchemaObject.tableMetadata();
  }

  public WhereCQLClauseAnalyzedResult analyse(DBLogicalExpression dbLogicalExpression) {

    Map<CqlIdentifier, TableFilter> identifierToFilter = new HashMap<>();
    consumeOnAllTableFilters(
        dbLogicalExpression,
        tableFilter -> {
          identifierToFilter.put(
              cqlIdentifierFromUserInput(tableFilter.path), tableFilter);
        });

    List<FilterException> filterExceptions = new ArrayList<>();
    List<WarningException> warningExceptions = new ArrayList<>();

    // TableFilter validation check rules, these will throw is there is an error
    checkAllColumnsExist(identifierToFilter);
    checkComparisonFilterAgainstDuration(identifierToFilter);

    // TableFilter warning check rules.
    List<WarningException> warnings = new ArrayList<>();
    warnMissingSaiOnFilterColumns(identifierToFilter).ifPresent(warnings::add);

    check$neOnIndexedColumn(warningExceptions, dbLogicalExpression);

    return new WhereCQLClauseAnalyzedResult(!warnings.isEmpty(), warnings);

    // TODO PK
  }

  /**
   * Validation check rule: validate if filtering columns exist
   *
   * <p>
   *
   * @param filterExceptions, list of collecting all FilterException
   * @param dbLogicalExpression, expression contains all TableFilters
   */
  private void checkAllColumnsExist(Map<CqlIdentifier, TableFilter> identifierToFilter) {

    var unknownColumns = identifierToFilter.keySet().stream()
        .filter(identifier -> !tableSchemaObject.tableMetadata().getColumns().containsKey(identifier))
        .toList();

    if (!unknownColumns.isEmpty()) {
      throw FilterException.Code.UNKNOWN_TABLE_COLUMNS.get(
              errVars(
                  tableSchemaObject,
                  map -> {
                    map.put("allColumns", errFmtColumnMetadata(tableMetadata.getColumns().values()));
                    map.put("unknownColumns", errFmtCqlIdentifier(unknownColumns));
                  }));
    }
  }

  /**
   * Validation check rule: validate if a comparison filter is against duration datatype column.
   *
   * <p>Can NOT apply $lt,$gt,$lte,$gte to duration datatype columns, because of "Slice restrictions
   * are not supported on duration columns".
   *
   * @param filterExceptions, collect all FilterException
   * @param dbLogicalExpression, expression contains all TableFilters
   */
  private void checkComparisonFilterAgainstDuration(Map<CqlIdentifier, TableFilter> identifierToFilter) {

    // assumed the checkAllColumnsExist has already run and the columns exist
    var filteredDurationColumns = identifierToFilter.entrySet().stream()
        .filter(entry -> {
          TableFilter tableFilter = entry.getValue();
          return (tableFilter instanceof NativeTypeTableFilter<?> nativeTypeTableFilter)
              && nativeTypeTableFilter.operator.isComparisonOperator();
        })
        .filter(entry -> {
          ColumnMetadata metadata = tableMetadata.getColumns().get(entry.getKey());
          return metadata.getType() == DataTypes.DURATION;
        })
        .map(Map.Entry::getKey)
        .toList();


    if (!filteredDurationColumns.isEmpty()) {
      throw FilterException.Code.COMPARISON_FILTER_AGAINST_DURATION_COLUMN.get(
              errVars(
                  tableSchemaObject,
                  map -> {
                    map.put(
                        "allColumns",
                        errFmtColumnMetadata(
                            tableSchemaObject.tableMetadata().getColumns().values()));
                    map.put(
                        "invalidDurationColumns", errFmtCqlIdentifier(filteredDurationColumns));
                  }));
    }
  }

  /**
   * Warning check rule: check if filter column is on SAI index.
   *
   * <p>Scalar datatype column: <br>
   * Without SAI index on column, ALLOW FILTERING is suggested. ($eq,$ne,$lt,$gt,$lte,$gte,$in TODO)
   *
   * <p>Collection datatype column: <br>
   * TODO
   *
   * @param warningExceptions, collect all warningException
   * @param dbLogicalExpression, expression contains all TableFilters
   */
  private Optional<WarningException> warnMissingSaiOnFilterColumns(
      Map<CqlIdentifier, TableFilter> identifierToFilter) {

    var missingSAIColumns =  identifierToFilter.keySet().stream()
        .filter(identifier -> !isIndexOnColumn(identifier))
        .toList();
    
    return missingSAIColumns.isEmpty() ? 
        Optional.empty() : 
        Optional.of(
            WarningException.Code.MISSING_SAI_INDEX.get(
                errVars(
                    tableSchemaObject,
                    map -> {
                      map.put("missingIndexColumns", errFmtCqlIdentifier(missingSAIColumns));
                    })));
  }


  /**
   * Warning check rule: $ne filter against column that is on SAI index but still needs ALLOW
   * FILTERING.
   *
   * <p>E.G. [perform $ne against a text column 'name' that has SAI index on it] <br>
   * Error from Driver: "Column 'name' has an index but does not support the operators specified in
   * the query. If you want to execute this query despite the performance unpredictability, use
   * ALLOW FILTERING"
   *
   * @param warningExceptions, list of collecting all warningExceptions
   * @param dbLogicalExpression, expression contains all TableFilters
   */
  private void check$neOnIndexedColumn(
      List<WarningException> warningExceptions, DBLogicalExpression dbLogicalExpression) {

    List<String> allowFiltering$neOnIndexedColumns = new ArrayList<>();
    Consumer<TableFilter> checkComparisonFilterRule =
        tableFilter -> {
          ColumnMetadata column =
              tableSchemaObject
                  .tableMetadata()
                  .getColumns()
                  .get(CqlIdentifier.fromInternal(tableFilter.path));

          if (isIndexOnColumn(tableFilter)
              && (tableFilter instanceof NativeTypeTableFilter nativeTypeTableFilter)) {
            if (nativeTypeTableFilter.operator == NativeTypeTableFilter.Operator.NE
                && ALLOW_FILTERING_NEEDED_$NE_DATATYPES_ON_SAI.contains(column.getType())) {
              allowFiltering$neOnIndexedColumns.add(tableFilter.path);
            }
          }
        };

    // consume by traverse all TableFilters
    consumeOnAllTableFilters(dbLogicalExpression, checkComparisonFilterRule);

    if (!allowFiltering$neOnIndexedColumns.isEmpty()) {
      warningExceptions.add(
          WarningException.Code.FILTER_NE_AGAINST_SAI_INDEXED_COLUMN_THAT_NEED_ALLOWING_FILTERING
              .get(
                  errVars(
                      tableSchemaObject,
                      map -> {
                        map.put(
                            "allowFilteringNeOnIndexedColumns",
                            errFmtFilterPath(allowFiltering$neOnIndexedColumns));
                      })));
    }
  }

  /**
   * The private helper method to recursively traverse the DBLogicalExpression. <br>
   * During the traverse, it will analyze each tableFilter per each rule
   *
   * @param dbLogicalExpression Logical relation container of DBFilters
   * @param analyzeRule consumer to analyze
   */
  public void consumeOnAllTableFilters(
      DBLogicalExpression dbLogicalExpression, Consumer<TableFilter> analyzeRule) {
    // traverse all dBFilters at current level of DBLogicalExpression
    for (DBFilterBase dbFilterBase : dbLogicalExpression.dBFilters()) {
      TableFilter tableFilter = (TableFilter) dbFilterBase;
      analyzeRule.accept(tableFilter);
    }
    // traverse sub dBLogicalExpression
    for (DBLogicalExpression subDBlogicalExpression : dbLogicalExpression.dbLogicalExpressions()) {
      consumeOnAllTableFilters(subDBlogicalExpression, analyzeRule);
    }
  }

  private static final Set<DataType> scalarDatatypes =
      Set.of(
          DataTypes.TEXT,
          DataTypes.ASCII,
          DataTypes.BOOLEAN,
          DataTypes.INT,
          DataTypes.DOUBLE,
          DataTypes.FLOAT,
          DataTypes.SMALLINT,
          DataTypes.TINYINT,
          DataTypes.BIGINT,
          DataTypes.VARINT,
          DataTypes.DECIMAL,
          DataTypes.TIMESTAMP,
          DataTypes.DATE,
          DataTypes.TIME,
          DataTypes.DURATION,
          DataTypes.BLOB);

  // Datatypes that need ALLOW FILTERING when column is on SAI and filtering on $ne
  private static final Set<DataType> ALLOW_FILTERING_NEEDED_$NE_DATATYPES_ON_SAI =
      Set.of(
          DataTypes.TEXT, DataTypes.ASCII, DataTypes.BOOLEAN, DataTypes.DURATION, DataTypes.BLOB);

  // Datatypes that do not need ALLOW FILTERING when column is on SAI and filtering on $ne
  private static final Set<DataType> ALLOW_FILTERING_NOT_NEEDED_$NE_DATATYPES_ON_SAI =
      Set.of(
          DataTypes.INT,
          DataTypes.DOUBLE,
          DataTypes.FLOAT,
          DataTypes.SMALLINT,
          DataTypes.TINYINT,
          DataTypes.BIGINT,
          DataTypes.VARINT,
          DataTypes.DECIMAL,
          DataTypes.TIMESTAMP,
          DataTypes.DATE,
          DataTypes.TIME);

  /**
   * This method will check the tableSchema and see if the filter column(path) has SAI index on it.
   *
   * @param tableFilter tableFilter
   * @return boolean to indicate if there is a SAI index on the column
   */
  private boolean isIndexOnColumn(CqlIdentifier column) {

    // NOTE: does not check the type of the secondary index, assuming all is SAI
    return tableMetadata().getIndexes().values().stream()
        .anyMatch(index -> index.getTarget().equals(column.asInternal()));
  }

  /**
   * This method will check the tableSchema and see if the filter column(path) is on the primary
   * key.
   *
   * @param tableFilter tableFilter
   * @return boolean to indicate if there is an index on the primary key
   */
  public boolean hasPrimaryKeyOnColumn(TableFilter tableFilter) {

    // Check if the column is a primary key (partition key or clustering column)
    boolean isPrimaryKey =
        tableSchemaObject.tableMetadata().getPartitionKey().stream()
                .anyMatch(column -> column.getName().equals(tableFilter.path))
            || tableSchemaObject.tableMetadata().getClusteringColumns().keySet().stream()
                .anyMatch(column -> column.getName().equals(tableFilter.path));
    return isPrimaryKey;
  }

  /**
   * Get the target columnMetaData if the column exists. Otherwise, throw V2 TABLE_COLUMN_UNKNOWN
   * Data API exception.
   *
   * @param tableFilter tableFilter
   * @return Optional<ColumnMetadata>, columnMetadata
   */
  public ColumnMetadata getColumn(TableFilter tableFilter) {

    ColumnMetadata column =
        tableSchemaObject
            .tableMetadata()
            .getColumns()
            .get(CqlIdentifier.fromInternal(tableFilter.path));
    if (column == null) {
      throw FilterException.Code.UNKNOWN_TABLE_COLUMNS.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put(
                    "allColumns",
                    errFmtColumnMetadata(tableSchemaObject.tableMetadata().getColumns().values()));
                map.put("unknownColumns", tableFilter.path);
              }));
    }

    return column;
  }
  
  /**
   * The analysis result of WhereCQLClause.
   *
   * <p>TODO this record is may not needed, keep it in case we want other information to be
   * returned.
   *
   * @param filterExceptions
   * @param warningExceptions
   */
  public record WhereCQLClauseAnalyzedResult(
      boolean requiresAllowFiltering, List<WarningException> warningExceptions) {}
}
