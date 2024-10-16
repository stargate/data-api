package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.NativeTypeTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDefs;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <b>NOTES:</b>
 *
 * <ul>
 *   <li>To make testing easier, any list of columns in the warnings are sorted by the column name.
 *       This is for the columns that failed some sort of test, the list of columns or PKs etc in
 *       the table are the order from the table metadata.
 * </ul>
 */
public class WhereCQLClauseAnalyzer {
  private static final Logger LOGGER = LoggerFactory.getLogger(WhereCQLClauseAnalyzer.class);

  // Datatypes that need ALLOW FILTERING even when there is a SAI on the column when Not Equals is
  // used.
  private static final Set<DataType> ALLOW_FILTERING_NEEDED_FOR_$NE =
      Set.of(
          DataTypes.TEXT,
          DataTypes.ASCII,
          DataTypes.BOOLEAN,
          DataTypes.DURATION,
          DataTypes.BLOB,
          DataTypes.UUID);

  // Datatypes that need ALLOW FILTERING even when there is a SAI on the column when <, >, <=, >= is
  // used
  private static final Set<DataType> ALLOW_FILTERING_NEEDED_FOR_COMPARISON =
      Set.of(DataTypes.TEXT, DataTypes.ASCII, DataTypes.BOOLEAN, DataTypes.UUID);

  private final TableBasedSchemaObject tableSchemaObject;
  private final TableMetadata tableMetadata;
  private final Map<CqlIdentifier, ColumnMetadata> tablePKColumns;

  public WhereCQLClauseAnalyzer(TableBasedSchemaObject tableSchemaObject) {
    this.tableSchemaObject =
        Objects.requireNonNull(tableSchemaObject, "tableSchemaObject cannot be null");

    tableMetadata = tableSchemaObject.tableMetadata();
    tablePKColumns =
        tableMetadata.getPrimaryKey().stream()
            .collect(Collectors.toMap(ColumnMetadata::getName, Function.identity()));
  }

  public WhereClauseAnalysis analyse(WhereCQLClause<?> whereCQLClause) {

    Map<CqlIdentifier, TableFilter> identifierToFilter = new HashMap<>();
    whereCQLClause
        .getLogicalExpression()
        .visitAllFilters(
            TableFilter.class,
            tableFilter -> {
              identifierToFilter.put(cqlIdentifierFromUserInput(tableFilter.path), tableFilter);
            });

    // TableFilter validation check rules, these will throw is there is an error
    checkAllColumnsExist(identifierToFilter);
    checkComparisonFilterAgainstDuration(identifierToFilter);

    // TableFilter warning check rules.
    List<WarningException> warnings = new ArrayList<>();

    warnMissingIndexOnScalar(identifierToFilter).ifPresent(warnings::add);
    warnNotEqUnsupportedByIndexing(identifierToFilter).ifPresent(warnings::add);
    warnComparisonUnsupportedByIndexing(identifierToFilter).ifPresent(warnings::add);
    warnNoFilters(identifierToFilter).ifPresent(warnings::add);
    warnPkNotFullySpecified(identifierToFilter).ifPresent(warnings::add);

    return new WhereClauseAnalysis(!warnings.isEmpty(), warnings);
  }

  /** Check all filters are on columns that exist in the table. */
  private void checkAllColumnsExist(Map<CqlIdentifier, TableFilter> identifierToFilter) {

    var unknownColumns =
        identifierToFilter.keySet().stream()
            .filter(
                identifier ->
                    !tableSchemaObject.tableMetadata().getColumns().containsKey(identifier))
            .sorted(CQL_IDENTIFIER_COMPARATOR)
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
   * Check the filter does not contain any comparison operations on duration data type
   *
   * <p>Can NOT apply $lt,$gt,$lte,$gte to duration datatype columns, returns error from DB "Slice
   * restrictions are not supported on duration columns".
   */
  private void checkComparisonFilterAgainstDuration(
      Map<CqlIdentifier, TableFilter> identifierToFilter) {

    // assumed the checkAllColumnsExist has already run and the columns exist
    var filteredDurationColumns =
        identifierToFilter.entrySet().stream()
            .filter(
                entry -> {
                  TableFilter tableFilter = entry.getValue();
                  return (tableFilter instanceof NativeTypeTableFilter<?> nativeTypeTableFilter)
                      && nativeTypeTableFilter.operator.isComparisonOperator();
                })
            .map((Map.Entry::getKey))
            .filter(
                column -> tableMetadata.getColumns().get(column).getType() == DataTypes.DURATION)
            .sorted(CQL_IDENTIFIER_COMPARATOR)
            .toList();

    if (!filteredDurationColumns.isEmpty()) {
      throw FilterException.Code.COMPARISON_FILTER_AGAINST_DURATION.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put(
                    "allColumns",
                    errFmtColumnMetadata(tableSchemaObject.tableMetadata().getColumns().values()));
                map.put("durationFilters", errFmtCqlIdentifier(filteredDurationColumns));
              }));
    }
  }

  /**
   * Warn if the filter is on columns that do not have an index on them, special handling for
   * primary key columns.
   *
   * <p>Only considers filters on the native, non collection types,
   */
  private Optional<WarningException> warnMissingIndexOnScalar(
      Map<CqlIdentifier, TableFilter> identifierToFilter) {

    var pkFullySpecified =
        identifierToFilter.keySet().stream().allMatch(tablePKColumns::containsKey);

    // if the Pk is fully specified, then we only check the filters on non-pk columns
    // otherwise we check all filters because the PK will not be used.
    var filtersToCheck =
        pkFullySpecified
            ? identifierToFilter.keySet().stream()
                .filter(tableFilter -> !tablePKColumns.containsKey(tableFilter))
                .toList()
            : identifierToFilter.keySet().stream().toList();

    var scalarTypeFilters =
        filtersToCheck.stream()
            .filter(
                identifier ->
                    ApiDataTypeDefs.PRIMITIVE_TYPES_BY_CQL_TYPE.containsKey(
                        tableMetadata.getColumns().get(identifier).getType()))
            .toList();

    var missingSAIColumns =
        scalarTypeFilters.stream()
            .filter(identifier -> !isIndexOnColumn(identifier))
            .sorted(CQL_IDENTIFIER_COMPARATOR)
            .toList();

    if (missingSAIColumns.isEmpty()) {
      return Optional.empty();
    }

    var indexedColumns =
        tableMetadata.getIndexes().values().stream()
            .map(IndexMetadata::getTarget)
            .sorted(String::compareTo)
            .toList();

    return Optional.of(
        WarningException.Code.MISSING_INDEX.get(
            errVars(
                tableSchemaObject,
                map -> {
                  map.put("primaryKey", errFmtColumnMetadata(tableMetadata.getPrimaryKey()));
                  map.put("indexedColumns", errFmtJoin(indexedColumns));
                  map.put("unindexedFilters", errFmtCqlIdentifier(missingSAIColumns));
                })));
  }

  /**
   * Warn if a filter is on a column that, while it has an index still needs ALLOW FILTERING because
   * not equals is used.
   *
   * <p>E.G. [perform $ne against a text column 'name' that has SAI index on it] <br>
   * Error from Driver: "Column 'name' has an index but does not support the operators specified in
   * the query. If you want to execute this query despite the performance unpredictability, use
   * ALLOW FILTERING"
   */
  private Optional<WarningException> warnNotEqUnsupportedByIndexing(
      Map<CqlIdentifier, TableFilter> identifierToFilter) {

    var inefficientFilters =
        identifierToFilter.entrySet().stream()
            .filter(
                entry -> {
                  TableFilter tableFilter = entry.getValue();
                  return (tableFilter instanceof NativeTypeTableFilter<?> nativeTypeTableFilter
                      && isIndexOnColumn(entry.getKey())
                      && nativeTypeTableFilter.operator == NativeTypeTableFilter.Operator.NE);
                })
            .map(Map.Entry::getKey)
            .filter(
                column ->
                    ALLOW_FILTERING_NEEDED_FOR_$NE.contains(
                        tableMetadata.getColumns().get(column).getType()))
            .sorted(CQL_IDENTIFIER_COMPARATOR)
            .toList();

    if (inefficientFilters.isEmpty()) {
      return Optional.empty();
    }

    var inefficientDataTypes =
        ALLOW_FILTERING_NEEDED_FOR_$NE.stream().map(DataType::toString).toList();

    var inefficientColumns =
        tableMetadata.getColumns().values().stream()
            .filter(column -> ALLOW_FILTERING_NEEDED_FOR_$NE.contains(column.getType()))
            .sorted(COLUMN_METADATA_COMPARATOR)
            .toList();

    return Optional.of(
        WarningException.Code.NOT_EQUALS_UNSUPPORTED_BY_INDEXING.get(
            errVars(
                tableSchemaObject,
                map -> {
                  map.put("inefficientDataTypes", errFmtJoin(inefficientDataTypes));
                  map.put("inefficientColumns", errFmtColumnMetadata(inefficientColumns));
                  map.put("inefficientFilters", errFmtCqlIdentifier(inefficientFilters));
                })));
  }

  /**
   * Warn if a filter is on a column that, while it has an index still needs ALLOW FILTERING because
   * comparison API operator $lt, $gt, $lte, $gte is used.
   *
   * <p>E.G. [perform $lt against a UUID column 'user_id' that has SAI index on it] <br>
   * Error from Driver: "Column 'user_id' has an index but does not support the operators specified
   * in the query. If you want to execute this query despite the performance unpredictability, use
   * ALLOW FILTERING" <br>
   * NOTE, TIMEUUID column does not have above constraint.
   */
  private Optional<WarningException> warnComparisonUnsupportedByIndexing(
      Map<CqlIdentifier, TableFilter> identifierToFilter) {

    var inefficientFilters =
        identifierToFilter.entrySet().stream()
            .filter(
                entry -> {
                  TableFilter tableFilter = entry.getValue();
                  return (tableFilter instanceof NativeTypeTableFilter<?> nativeTypeTableFilter
                      && isIndexOnColumn(entry.getKey())
                      && nativeTypeTableFilter.operator.isComparisonOperator());
                })
            .map(Map.Entry::getKey)
            .filter(
                column ->
                    ALLOW_FILTERING_NEEDED_FOR_COMPARISON.contains(
                        tableMetadata.getColumns().get(column).getType()))
            .sorted(CQL_IDENTIFIER_COMPARATOR)
            .toList();

    if (inefficientFilters.isEmpty()) {
      return Optional.empty();
    }

    var inefficientDataTypes =
        ALLOW_FILTERING_NEEDED_FOR_COMPARISON.stream().map(DataType::toString).toList();

    var inefficientColumns =
        tableMetadata.getColumns().values().stream()
            .filter(column -> ALLOW_FILTERING_NEEDED_FOR_COMPARISON.contains(column.getType()))
            .sorted(COLUMN_METADATA_COMPARATOR)
            .toList();

    return Optional.of(
        WarningException.Code.COMPARISON_FILTER_UNSUPPORTED_BY_INDEXING.get(
            errVars(
                tableSchemaObject,
                map -> {
                  map.put("inefficientDataTypes", errFmtJoin(inefficientDataTypes));
                  map.put("inefficientColumns", errFmtColumnMetadata(inefficientColumns));
                  map.put("inefficientFilterColumns", errFmtCqlIdentifier(inefficientFilters));
                })));
  }

  private Optional<WarningException> warnNoFilters(
      Map<CqlIdentifier, TableFilter> identifierToFilter) {

    if (!identifierToFilter.isEmpty()) {
      return Optional.empty();
    }

    var indexedColumns =
        tableMetadata.getIndexes().values().stream().map(IndexMetadata::getTarget).toList();

    return Optional.of(
        WarningException.Code.ZERO_FILTER_OPERATIONS.get(
            errVars(
                tableSchemaObject,
                map -> {
                  map.put("primaryKey", errFmtColumnMetadata(tableMetadata.getPrimaryKey()));
                  map.put("indexedColumns", errFmtJoin(indexedColumns));
                })));
  }

  private Optional<WarningException> warnPkNotFullySpecified(
      Map<CqlIdentifier, TableFilter> identifierToFilter) {

    // If there are no filters, it is handled by the warnNoFilters method
    // this is only for OK is partially filtered
    if (identifierToFilter.isEmpty()) {
      return Optional.empty();
    }
    var allFiltersOnPkColumns =
        identifierToFilter.keySet().stream().allMatch(tablePKColumns::containsKey);
    // this rule only applies if all the filters are on PK columns
    if (!allFiltersOnPkColumns) {
      return Optional.empty();
    }

    var missingPartitionKeyMetadata =
        tableMetadata.getPartitionKey().stream()
            .filter(column -> !identifierToFilter.containsKey(column.getName()))
            .sorted(COLUMN_METADATA_COMPARATOR)
            .toList();

    // If we are filtering on any clustering keys, then we need to make sure we are not skipping any
    // i.e. if clustering is (a,b,c) and we are filtering on (a,c) then we are skipping b
    final boolean[] skipped = {false};
    var noClusteringKeyFilters =
        tableMetadata.getClusteringColumns().keySet().stream()
            .noneMatch(metadata -> identifierToFilter.containsKey(metadata.getName()));

    List<ColumnMetadata> outOfOrderClusteringKeys =
        noClusteringKeyFilters
            ? Collections.emptyList()
            : tableMetadata.getClusteringColumns().keySet().stream()
                .filter(
                    metadata -> {
                      if (identifierToFilter.containsKey(metadata.getName())) {
                        // if we have a filter for this clustering key, then we must not have
                        // skipped any previously
                        // we are out of order if we have skipped any previously
                        return skipped[0];
                      }
                      // we have skipped this clustering key that is OK
                      // just need to remember we skipped one incase we try to set another later
                      skipped[0] = true;
                      return false;
                    })
                .sorted(COLUMN_METADATA_COMPARATOR)
                .toList();

    if (missingPartitionKeyMetadata.isEmpty() && outOfOrderClusteringKeys.isEmpty()) {
      return Optional.empty();
    }

    // the filter is only on the PK columns, but does not specify the full PK
    return Optional.of(
        WarningException.Code.INCOMPLETE_PRIMARY_KEY_FILTER.get(
            errVars(
                tableSchemaObject,
                map -> {
                  map.put("primaryKeys", errFmtColumnMetadata(tableMetadata.getPrimaryKey()));
                  map.put(
                      "missingPartitionKeys", errFmtColumnMetadata(missingPartitionKeyMetadata));
                  map.put(
                      "outOfOrderClusteringKeys", errFmtColumnMetadata(outOfOrderClusteringKeys));
                })));
  }

  private boolean isIndexOnColumn(CqlIdentifier column) {
    // NOTE: does not check the type of the secondary index, assuming all is SAI
    // have to use list because the indexes are keyed on the index name, not the column name.
    // TODO: confirm it is OK to not check the index type and properties
    return tableMetadata.getIndexes().values().stream()
        .anyMatch(index -> index.getTarget().equals(column.asInternal()));
  }

  /**
   * The result of the where clause analysis.
   *
   * @param requiresAllowFiltering TRUE if the query requires ALLOW FILTERING, FALSE otherwise.
   * @param warningExceptions List of warning exceptions that should be outputted with the query,
   *     NOTE there may be warnings without needing ALLOW FILTERING.
   */
  public record WhereClauseAnalysis(
      boolean requiresAllowFiltering, List<WarningException> warningExceptions) {

    public WhereClauseAnalysis {
      warningExceptions =
          warningExceptions == null ? List.of() : Collections.unmodifiableList(warningExceptions);
    }

    /**
     * Helper to check if the analysis is empty
     *
     * @return TRUE if allow filtering is not required and there are no warnings, FALSE otherwise.
     */
    public boolean isEmpty() {
      return !requiresAllowFiltering && warningExceptions.isEmpty();
    }
  }
}
