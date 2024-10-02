package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtColumnMetadata;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.InTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.NativeTypeTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilter;
import java.util.*;

/**
 * The purpose for this analyzer class is to analyze the whereCQLClause.
 *
 * <p>TODO, the analysis flow is not determined yet. It will traverse the DBLogicalExpression and
 * analyze the individual {@link TableFilter}. Then analyze the primary key filtering. It finally
 * gives an overall analysis result.
 *
 * <p>TODO, this class will be helpful for future features, such as TableFilter and Index
 * recommendation.
 */
public class WhereCQLClauseAnalyzer {

  private final TableSchemaObject tableSchemaObject;

  public WhereCQLClauseAnalyzer(TableSchemaObject tableSchemaObject) {
    this.tableSchemaObject = tableSchemaObject;
  }

  /**
   * The entrance of analyzing process for WhereCQLClause.
   *
   * <p>Step 1:<br>
   * Recursively traverse through the DBLogicalExpression, analyze individual TableFilter. <br>
   * The result of analyze individual TableFilter will be should ALLOW FILTERING be turned on.
   *
   * <p>Step 2:<br>
   * TODO
   */
  public WhereCQLClauseAnalyzedResult analyse(DBLogicalExpression dbLogicalExpression) {

    // Step 1: Analyze individual TableFilter first.
    List<TableFilterAnalyzedResult> tableFilterAnalyzedResults = new ArrayList<>();
    traverseDBLogicalExpression(dbLogicalExpression, tableFilterAnalyzedResults);

    // Step 2: PK
    // TODO

    // Step 3: Make decision.

    boolean withAllowFiltering = false;
    List<String> warnings = new ArrayList<>();
    for (TableFilterAnalyzedResult tableFilterAnalyzedResult : tableFilterAnalyzedResults) {
      withAllowFiltering |= tableFilterAnalyzedResult.withAllowFiltering();
      tableFilterAnalyzedResult
          .warning()
          .ifPresent(warningException -> warnings.add(warningException.getMessage()));
    }

    return new WhereCQLClauseAnalyzedResult(withAllowFiltering, warnings);
  }

  /**
   * The private helper method to recursively traverse the DBLogicalExpression. <br>
   * During the traverse, it will call the to analyze individual TableFilter
   *
   * @param dbLogicalExpression Logical relation container of DBFilters
   * @param dbFilterUsages List of analyse result for DBFilters
   */
  private void traverseDBLogicalExpression(
      DBLogicalExpression dbLogicalExpression,
      List<TableFilterAnalyzedResult> tableFilterAnalyzedResults) {

    // traverse all dBFilters at current level of DBLogicalExpression
    for (DBFilterBase dbFilterBase : dbLogicalExpression.dBFilters()) {
      TableFilter tableFilter = (TableFilter) dbFilterBase;
      tableFilterAnalyzedResults.add(analyzeIndividualTableFilter(tableFilter));
    }
    // traverse sub dBLogicalExpression
    for (DBLogicalExpression subDBlogicalExpression : dbLogicalExpression.dbLogicalExpressions()) {
      traverseDBLogicalExpression(subDBlogicalExpression, tableFilterAnalyzedResults);
    }
  }

  /**
   * Analyze individual TableFilter and return TableFilterAnalyzedResult.
   *
   * @param tableFilter, target TableFilter to be analyzed
   * @return TableFilterAnalyzedResult, analyzed result for individual TableFilter
   */
  private TableFilterAnalyzedResult analyzeIndividualTableFilter(TableFilter tableFilter) {

    // Analyze if tableFilter is a NativeTypeTableFilter
    if (tableFilter instanceof NativeTypeTableFilter nativeTypeTableFilter) {
      return analyseNativeTypeTableFilter(nativeTypeTableFilter);
    }

    // Analyze if tableFilter is an InTableFilter
    if (tableFilter instanceof InTableFilter inTableFilter) {
      return analyseInTableFilter(inTableFilter);
    }

    return new TableFilterAnalyzedResult(false, Optional.empty());
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

  // Datatypes that need ALLOW FILTERING when column is on SAI when filtering on $ne
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
   * Analyse a NativeTypeTableFilter and return TableFilterAnalyzedResult.<br>
   *
   * <p>check 1: If filter is against an existing column, get the columnMetadata.
   *
   * <p>check 2: Operator $eq.<br>
   * For all scalar data types: <br>
   * --- If without SAI, ALLOW FILTERING is needed. <br>
   * --- Above rule also covers Duration and Blob, since both can NOT have SAI. TODO Collection
   * Types.
   *
   * <p>check 3: Operator $ne.<br>
   * For all scalar data types: <br>
   * --- If without SAI, ALLOW FILTERING is needed. <br>
   * --- With SAI, Datatypes in ALLOW_FILTERING_NEEDED_$NE_DATATYPES_ON_SAI need ALLOW FILTERING.
   * <br>
   * --- With SAI, DataTypes in ALLOW_FILTERING_NOT_NEEDED_$NE_DATATYPES_ON_SAI do NOT need ALLOW
   * FILTERING <br>
   * TODO Collection Types
   *
   * <p>check 4: Operator $gt,$lt,$gte,$lte.<br>
   * For all scalar data types: <br>
   * --- Duration column can not perform $gt,$lt,$gte,$lte, since "Slice restrictions are not
   * supported on duration columns" <br>
   * --- If without SAI, ALLOW FILTERING is needed. <br>
   * TODO Collection Types
   *
   * @param nativeTypeTableFilter nativeTypeTableFilter
   * @return TableFilterAnalyzedResult
   */
  private TableFilterAnalyzedResult analyseNativeTypeTableFilter(
      NativeTypeTableFilter nativeTypeTableFilter) {
    // Check 1, if column exists
    final ColumnMetadata column = getColumn(nativeTypeTableFilter);
    final boolean hasSaiIndexOnColumn = hasSaiIndexOnColumn(nativeTypeTableFilter);
    final DataType dataType = column.getType();
    final String predicateCql = nativeTypeTableFilter.operator.predicate.cql;
    final String columnName = nativeTypeTableFilter.path;

    // Check 2, operator $eq
    if (nativeTypeTableFilter.operator == NativeTypeTableFilter.Operator.EQ) {
      // If column is not on SAI, turn on ALLOW FILTERING.
      if (!hasSaiIndexOnColumn) {
        return TableFilterAnalyzedResult.needAllowFiltering(
            tableSchemaObject,
            String.format(
                "'%s' against column '%s' that is NOT on SAI index", predicateCql, columnName));
      }
    }

    // Check 3, operator $ne
    if (nativeTypeTableFilter.operator == NativeTypeTableFilter.Operator.NE) {
      // If column is not on SAI, turn on ALLOW FILTERING.
      if (!hasSaiIndexOnColumn) {
        return TableFilterAnalyzedResult.needAllowFiltering(
            tableSchemaObject,
            String.format(
                "'%s' against column '%s' that is NOT on SAI index", predicateCql, columnName));
      }
      // If column is on SAI and column type is one of ALLOW_FILTERING_NEEDED_$NE_DATATYPES_ON_SAI,
      // turn on ALLOW FILTERING
      if (hasSaiIndexOnColumn && ALLOW_FILTERING_NEEDED_$NE_DATATYPES_ON_SAI.contains(dataType)) {
        return TableFilterAnalyzedResult.needAllowFiltering(
            tableSchemaObject,
            String.format(
                "'%s' is not supported for datatype '%s' on column '%s'",
                predicateCql, dataType, columnName));
      }
    }

    // check 4, operator $lt,$gt,$lte,$gte
    if (NativeTypeTableFilter.Operator.isComparisonOperator(nativeTypeTableFilter.operator)) {

      // Check Duration type first, can not perform comparison API filter since "Slice restrictions
      // are not supported on duration columns"
      if (dataType == DataTypes.DURATION) {
        throw FilterException.Code.INVALID_FILTER.get(
            errVars(
                tableSchemaObject,
                map -> {
                  map.put(
                      "filter",
                      String.format(
                          "'%s' is not supported on duration column '%s', Slice restrictions are not supported on duration columns.",
                          predicateCql, columnName));
                }));
      }

      if (!hasSaiIndexOnColumn) {
        // This covers Blob column, since C* can not build SAI on Blob column.
        return TableFilterAnalyzedResult.needAllowFiltering(
            tableSchemaObject,
            String.format(
                "'%s' against column '%s' that is NOT on SAI index", predicateCql, columnName));
      }
    }

    return new TableFilterAnalyzedResult(false, Optional.empty());
  }

  /**
   * Analyse an InTableFilter and returned TableFilterAnalyzedResult.<br>
   *
   * <p>check 1: If filter is against an existing column, get the columnMetadata.
   *
   * <p>check 2: scalar data types <br>
   * --- If without SAI, ALLOW FILTERING is needed. <br>
   * --- Above rule also covers Duration and Blob, since both can NOT have SAI. TODO Collection
   * Types.
   *
   * @param InTableFilter inTableFilter
   * @return TableFilterAnalyzedResult
   */
  private TableFilterAnalyzedResult analyseInTableFilter(InTableFilter inTableFilter) {
    // Check 1: If filter is against an existing column
    final ColumnMetadata column = getColumn(inTableFilter);
    final boolean hasSaiIndexOnColumn = hasSaiIndexOnColumn(inTableFilter);
    final String predicateCql = inTableFilter.operator.predicate.cql;
    final String columnName = inTableFilter.path;

    // Check 2:
    if (!hasSaiIndexOnColumn && scalarDatatypes.contains(column.getType())) {
      return TableFilterAnalyzedResult.needAllowFiltering(
          tableSchemaObject,
          String.format(
              "'%s' against column '%s' that is NOT on SAI index", predicateCql, columnName));
    }

    return new TableFilterAnalyzedResult(false, Optional.empty());
    // TODO, Against a collection column.
  }

  /**
   * This method will check the tableSchema and see if the filter column(path) has SAI index on it.
   *
   * @param TableFilter tableFilter
   * @return boolean to indicate if there is a SAI index on the column
   */
  private boolean hasSaiIndexOnColumn(TableFilter tableFilter) {

    // Check if the column has SAI index on it
    return tableSchemaObject.tableMetadata().getIndexes().values().stream()
        .anyMatch(index -> index.getTarget().equals(tableFilter.path));
  }

  /**
   * This method will check the tableSchema and see if the filter column(path) is on the primary
   * key.
   *
   * @param TableFilter tableFilter
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
   * @param TableFilter tableFilter
   * @return Optional<ColumnMetadata>, columnMetadata
   */
  public ColumnMetadata getColumn(TableFilter tableFilter) {
    return tableSchemaObject.tableMetadata().getColumns().entrySet().stream()
        .filter(entry -> entry.getKey().asInternal().equals(tableFilter.path))
        .map(Map.Entry::getValue)
        .findFirst()
        .orElseThrow(
            () ->
                FilterException.Code.UNKNOWN_TABLE_COLUMNS.get(
                    errVars(
                        tableSchemaObject,
                        map -> {
                          map.put(
                              "allColumns",
                              errFmtColumnMetadata(
                                  tableSchemaObject.tableMetadata().getColumns().values()));
                          map.put("unknownColumns", tableFilter.path);
                        })));
  }

  /**
   * The analyzed result of individual TableFilter
   *
   * @param withAllowFiltering
   * @param warning
   */
  public record TableFilterAnalyzedResult(
      boolean withAllowFiltering, Optional<WarningException> warning) {

    public static TableFilterAnalyzedResult needAllowFiltering(
        TableSchemaObject tableSchemaObject, String allowFilteringReason) {
      return new TableFilterAnalyzedResult(
          true,
          Optional.of(
              WarningException.Code.ALLOW_FILTERING.get(
                  errVars(
                      tableSchemaObject,
                      map -> {
                        map.put("reason", allowFilteringReason);
                      }))));
    }

    public static TableFilterAnalyzedResult noAllowFiltering() {
      return new TableFilterAnalyzedResult(false, Optional.empty());
    }
  }

  /**
   * @param withAllowFiltering
   * @param warnings
   */
  public record WhereCQLClauseAnalyzedResult(boolean withAllowFiltering, List<String> warnings) {}
}
