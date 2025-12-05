package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtCqlIdentifier;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableBasedSchemaObject;import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.InTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.MapSetListFilterComponent;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.MapSetListTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.NativeTypeTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexFunction;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTableDef;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

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
  /** Different statementTypes for analyzer to work on, thus different strategies. */
  public enum StatementType {
    SELECT,
    // currently, table feature only supports updateOne
    UPDATE_ONE,
    DELETE_ONE,
    DELETE_MANY;

    AnalysisStrategy getStrategy(WhereCQLClauseAnalyzer analyzer) {

      // NOTE: the order we run the checks in is important, because the throw if there is a problem
      // stopping
      // processing the filters, all warnings are run though.
      return switch (this) {
        case SELECT ->
            new AnalysisStrategy(
                List.of(
                    analyzer::checkAllColumnsExist,
                    analyzer::checkLexicalFilterOnlyOnLexicalIndex,
                    analyzer::checkComparisonFilterAgainstDuration),
                List.of(
                    analyzer::warnMissingIndexOnScalar,
                    analyzer::warnMissingIndexOnMapSetList,
                    analyzer::warnNotEqUnsupportedByIndexing,
                    analyzer::warnComparisonUnsupportedByIndexing,
                    analyzer::warnNotInUnsupportedByIndexing,
                    analyzer::warnNoFilters,
                    analyzer::warnPkNotFullySpecified));
        case DELETE_ONE, UPDATE_ONE ->
            new AnalysisStrategy(
                List.of(
                    analyzer::checkNoFilters,
                    analyzer::checkAllColumnsExist,
                    analyzer::checkNonPrimaryKeyFilters,
                    analyzer::checkOnlyEqUsage,
                    analyzer::checkFullPrimaryKey),
                List.of());
        case DELETE_MANY ->
            new AnalysisStrategy(
                List.of(
                    analyzer::checkNoFilters,
                    analyzer::checkAllColumnsExist,
                    analyzer::checkNonPrimaryKeyFilters,
                    analyzer::checkValidPrimaryKey),
                List.of());
      };
    }
  }

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

  // Datatypes that need ALLOW FILTERING even when there is a SAI on the column when NOT IN is
  // used
  private static final Set<DataType> ALLOW_FILTERING_NEEDED_FOR_NOT_IN =
      Set.of(DataTypes.TEXT, DataTypes.ASCII, DataTypes.BOOLEAN, DataTypes.UUID);

  private final TableBasedSchemaObject tableSchemaObject;
  private final TableMetadata tableMetadata;
  private final ApiTableDef apiTableDef;
  private final StatementType statementType;

  public WhereCQLClauseAnalyzer(TableSchemaObject tableSchemaObject, StatementType statementType) {
    this.tableSchemaObject =
        Objects.requireNonNull(tableSchemaObject, "tableSchemaObject cannot be null");
    this.statementType = Objects.requireNonNull(statementType, "statementType cannot be null");

    this.tableMetadata = tableSchemaObject.tableMetadata();
    this.apiTableDef = tableSchemaObject.apiTableDef();
  }

  /**
   * Analyse the provided where clause, throwing errors for invalid combinations and generating
   * warnings where needed.
   *
   * @param whereCQLClause the where clause to analyse
   * @return A {@link WhereClauseWithWarnings} object containing the results of the analysis,
   *     including whether the query requires ALLOW FILTERING and any warnings generated.
   */
  public WhereClauseWithWarnings analyse(WhereCQLClause<?> whereCQLClause) {

    Objects.requireNonNull(whereCQLClause, "whereCQLClause cannot be null");

    Map<CqlIdentifier, TableFilter> identifierToFilter = new HashMap<>();
    whereCQLClause
        .getLogicalExpression()
        .visitAllFilters(
            TableFilter.class,
            tableFilter -> {
              identifierToFilter.put(cqlIdentifierFromUserInput(tableFilter.path), tableFilter);
            });

    var strategy = statementType.getStrategy(this);
    // Apply validation rules from analyzeStrategy (throw if invalid)
    strategy.validationCheckRules.forEach(rule -> rule.accept(identifierToFilter));

    // Apply warning rules from analyzeStrategy
    List<WarningException> warnings = new ArrayList<>();
    strategy.warningCheckRules.forEach(
        warningRule -> warningRule.apply(identifierToFilter).ifPresent(warnings::add));

    return new WhereClauseWithWarnings(whereCQLClause, warnings, !warnings.isEmpty());
  }

  /**
   * Check if there is no filters, call it invalid filtering for UPDATE and DELETE.
   *
   * <p>Note, SELECT does not use this rule, table scan if no filter (So still valid filtering).
   */
  private void checkNoFilters(Map<CqlIdentifier, TableFilter> identifierToFilter) {
    if (identifierToFilter.isEmpty()) {
      throw FilterException.Code.MISSING_FILTER_FOR_UPDATE_DELETE.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put(
                    "primaryKeyColumns",
                    errFmtColumnMetadata(tableSchemaObject.tableMetadata().getPrimaryKey()));
              }));
    }
  }

  /**
   * Check if there is other columns are filtered against other than primary key columns.
   *
   * <p>For UPDATE, DELETE. If there are additional columns are specified in the where clause other
   * than primary key columns, [Invalid query] message="Non PRIMARY KEY columns found in where
   * clause: xxx"
   */
  private void checkNonPrimaryKeyFilters(Map<CqlIdentifier, TableFilter> identifierToFilter) {

    var nonPkFilters =
        identifierToFilter.keySet().stream()
            .filter(identifier -> !apiTableDef.primaryKeys().containsKey(identifier))
            .sorted(CQL_IDENTIFIER_COMPARATOR)
            .toList();

    if (!nonPkFilters.isEmpty()) {
      throw FilterException.Code.UNSUPPORTED_NON_PRIMARY_KEY_FILTER_FOR_UPDATE_DELETE.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("primaryKeyColumns", errFmtColumnMetadata(tableMetadata.getPrimaryKey()));
                map.put("nonPrimaryKeyFilters", errFmtCqlIdentifier(nonPkFilters));
              }));
    }
  }

  /** Check all filters are on columns that exist in the table. */
  private void checkAllColumnsExist(Map<CqlIdentifier, TableFilter> identifierToFilter) {

    var unknownColumns =
        identifierToFilter.keySet().stream()
            .filter(identifier -> !tableMetadata.getColumns().containsKey(identifier))
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

  /** Check that all lexical filters are on columns with lexical index. */
  private void checkLexicalFilterOnlyOnLexicalIndex(
      Map<CqlIdentifier, TableFilter> identifierToFilter) {
    List<NativeTypeTableFilter> lexicalFilters =
        identifierToFilter.values().stream()
            .filter(NativeTypeTableFilter.class::isInstance)
            .map(NativeTypeTableFilter.class::cast)
            .filter(f -> f.operator == NativeTypeTableFilter.Operator.TEXT_SEARCH)
            .toList();
    var indexes = apiTableDef.indexes();
    var lexicalFilterColumnsWithoutIndex =
        lexicalFilters.stream()
            .filter(
                f -> {
                  Optional<ApiIndexDef> maybeIndex = indexes.firstIndexFor(f.getColumn());
                  return maybeIndex.isEmpty() || maybeIndex.get().indexType() != ApiIndexType.TEXT;
                })
            .map(NativeTypeTableFilter::getColumn)
            .toList();
    if (!lexicalFilterColumnsWithoutIndex.isEmpty()) {
      var textIndexedColumns =
          indexes.allIndexes().stream()
              .filter(index -> index.indexType() == ApiIndexType.TEXT)
              .map(ApiIndexDef::targetColumn)
              .toList();
      throw FilterException.Code.CANNOT_LEXICAL_FILTER_NON_INDEXED_COLUMNS.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("indexedColumns", errFmtCqlIdentifier(textIndexedColumns));
                map.put("filterColumns", errFmtCqlIdentifier(lexicalFilterColumnsWithoutIndex));
              }));
    }
  }

  /**
   * Check if any non $eq filter is used.
   *
   * <p>e.g. Table UpdateOne and DeleteOne, DATA API does not know the exact modified row count in
   * DB, so it needs to restrict on the filters. Make sure only $eq is used for DeleteOne and
   * UpdateOne.
   */
  private void checkOnlyEqUsage(Map<CqlIdentifier, TableFilter> identifierToFilter) {

    var nonEqFilterColumns =
        identifierToFilter.entrySet().stream()
            .filter(
                entry -> {
                  TableFilter tableFilter = entry.getValue();
                  if (!(tableFilter instanceof NativeTypeTableFilter<?> nativeTypeTableFilter)) {
                    return true;
                  }
                  return nativeTypeTableFilter.operator != NativeTypeTableFilter.Operator.EQ;
                })
            .map(Map.Entry::getKey)
            .sorted(CQL_IDENTIFIER_COMPARATOR)
            .toList();

    if (!nonEqFilterColumns.isEmpty()) {
      throw FilterException.Code.UNSUPPORTED_FILTER_FOR_UPDATE_ONE_DELETE_ONE.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("unsupportedFilterColumns", errFmtCqlIdentifier(nonEqFilterColumns));
              }));
    }
  }

  /**
   * Check if all primary keys components (partition keys, clustering keys) are specified in filter.
   *
   * <p>For Update statement, if not a full primary key filtering is specified, it is not a valid
   * filtering. The WHERE clause is used to select the row to update and must include all columns of
   * the `PRIMARY KEY`.
   * https://cassandra.apache.org/doc/4.1/cassandra/cql/dml.html#update-statement. <br>
   * For DELETE_ONE (API defined), user must specify the full primary key in the filter.
   */
  private void checkFullPrimaryKey(Map<CqlIdentifier, TableFilter> identifierToFilter) {

    var missingPKColumns =
        tableMetadata.getPrimaryKey().stream()
            .filter(pkColumn -> !identifierToFilter.containsKey(pkColumn.getName()))
            .sorted(COLUMN_METADATA_COMPARATOR)
            .toList();

    if (!missingPKColumns.isEmpty()) {
      throw FilterException.Code.MISSING_FULL_PRIMARY_KEY_FOR_UPDATE_DELETE.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("primaryKeyColumns", errFmtColumnMetadata(tableMetadata.getPrimaryKey()));
                map.put("missingPrimaryKeyFilters", errFmtColumnMetadata(missingPKColumns));
              }));
    }
  }

  /**
   * Check if a valid primary key filter (all partition keys, non-skipping clustering keys) are
   * specified in filter.
   *
   * <p>For DELETE_MANY statement, partial primary key is allowed (still needs to be a valid one).
   */
  private void checkValidPrimaryKey(Map<CqlIdentifier, TableFilter> identifierToFilter) {
    var missingPartitionKeys = missingPartitionKeys(identifierToFilter);
    var outOfOrderClusteringKeys = outOfOrderClusteringKeys(identifierToFilter);

    if (!missingPartitionKeys.isEmpty() || !outOfOrderClusteringKeys.isEmpty()) {
      throw FilterException.Code.INVALID_PRIMARY_KEY_FILTER.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("primaryKeys", errFmtColumnMetadata(tableMetadata.getPrimaryKey()));
                map.put("missingPartitionKeys", errFmtColumnMetadata(missingPartitionKeys));
                map.put("outOfOrderClusteringKeys", errFmtColumnMetadata(outOfOrderClusteringKeys));
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
            .filter(entry -> entry.getValue().filterIsSlice())
            .map((Map.Entry::getKey))
            .filter(
                column -> tableMetadata.getColumns().get(column).getType() == DataTypes.DURATION)
            .sorted(CQL_IDENTIFIER_COMPARATOR)
            .toList();

    if (!filteredDurationColumns.isEmpty()) {
      throw FilterException.Code.UNSUPPORTED_COMPARISON_FILTER_AGAINST_DURATION.get(
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
   * Warn if the filter is on scalar columns that do not have an index on them, special handling for
   * primary key columns.
   *
   * <p>Only considers filters on the native, not on map/set/list types,
   */
  private Optional<WarningException> warnMissingIndexOnScalar(
      Map<CqlIdentifier, TableFilter> identifierToFilter) {

    var pkFullySpecified =
        identifierToFilter.keySet().stream()
            .allMatch(identifier -> apiTableDef.primaryKeys().containsKey(identifier));

    // if the Pk is fully specified, then we only check the filters on non-pk columns
    // otherwise we check all filters because the PK will not be used.
    var filtersToCheck =
        pkFullySpecified
            ? identifierToFilter.keySet().stream()
                .filter(filterColumn -> !apiTableDef.primaryKeys().containsKey(filterColumn))
                .toList()
            : identifierToFilter.keySet().stream().toList();

    var scalarTypeFilters =
        filtersToCheck.stream()
            .filter(identifier -> apiTableDef.allColumns().get(identifier).type().isPrimitive())
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
   * Warn if the filter is on map/set/list columns that do not have an index on them.
   *
   * <p>Only frozen map/set/list can be part of primary key in Cassandra. But still needs additional
   * SAI index to use Data API map/set/list filtering.
   */
  private Optional<WarningException> warnMissingIndexOnMapSetList(
      Map<CqlIdentifier, TableFilter> identifierToFilter) {

    var mapSetListFilters =
        identifierToFilter.entrySet().stream()
            .filter(entry -> entry.getValue() instanceof MapSetListTableFilter)
            .map(entry -> Map.entry(entry.getKey(), (MapSetListTableFilter) entry.getValue()))
            .toList();

    // filter out the map/set/list column that user wants to filter on, but does not have the index
    // or does not
    // have the correct index function on it.
    var missingIndexColumns =
        mapSetListFilters.stream()
            .filter(
                entry ->
                    !isIndexOnMapSetList(entry.getKey(), entry.getValue().getFilterComponent()))
            .map(Map.Entry::getKey)
            .sorted(CQL_IDENTIFIER_COMPARATOR)
            .toList();

    if (missingIndexColumns.isEmpty()) {
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
                  map.put("unindexedFilters", errFmtCqlIdentifier(missingIndexColumns));
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
                  return isIndexOnColumn(entry.getKey()) && entry.getValue().filterIsSlice();
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

  /**
   * Warn if a filter is on a column that, while it has an index still needs ALLOW FILTERING because
   * NOT IN operator is used.
   *
   * <p>E.G. [perform $nin against a TEXT column 'name' that has SAI index on it] <br>
   * Error from Driver: "Column 'name' has an index but does not support the operators specified in
   * the query. If you want to execute this query despite the performance unpredictability, use
   * ALLOW FILTERING" <br>
   * NOTE, TIMEUUID column does not have above constraint.
   */
  private Optional<WarningException> warnNotInUnsupportedByIndexing(
      Map<CqlIdentifier, TableFilter> identifierToFilter) {

    var inefficientFilters =
        identifierToFilter.entrySet().stream()
            .filter(
                entry -> {
                  TableFilter tableFilter = entry.getValue();
                  return (tableFilter instanceof InTableFilter inTableFilter
                      && inTableFilter.operator == InTableFilter.Operator.NIN
                      && isIndexOnColumn(entry.getKey()));
                })
            .map(Map.Entry::getKey)
            .filter(
                column ->
                    ALLOW_FILTERING_NEEDED_FOR_NOT_IN.contains(
                        tableMetadata.getColumns().get(column).getType()))
            .sorted(CQL_IDENTIFIER_COMPARATOR)
            .toList();

    if (inefficientFilters.isEmpty()) {
      return Optional.empty();
    }

    var inefficientDataTypes =
        ALLOW_FILTERING_NEEDED_FOR_NOT_IN.stream().map(DataType::toString).toList();

    var inefficientColumns =
        tableMetadata.getColumns().values().stream()
            .filter(column -> ALLOW_FILTERING_NEEDED_FOR_NOT_IN.contains(column.getType()))
            .sorted(COLUMN_METADATA_COMPARATOR)
            .toList();

    return Optional.of(
        WarningException.Code.NOT_IN_FILTER_UNSUPPORTED_BY_INDEXING.get(
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
        identifierToFilter.keySet().stream()
            .allMatch(identifier -> apiTableDef.primaryKeys().containsKey(identifier));
    // this rule only applies if all the filters are on PK columns
    if (!allFiltersOnPkColumns) {
      return Optional.empty();
    }

    var missingPartitionKeyMetadata = missingPartitionKeys(identifierToFilter);
    var outOfOrderClusteringKeys = outOfOrderClusteringKeys(identifierToFilter);

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

  // ==================================================================================================================
  // Some helper methods can be commonly used by rules.
  // ==================================================================================================================

  private boolean isIndexOnColumn(CqlIdentifier column) {
    // NOTE: does not check the type of the secondary index, assuming all is SAI
    // have to use list because the indexes are keyed on the index name, not the column name.
    // TODO: confirm it is OK to not check the index type and properties
    return tableMetadata.getIndexes().values().stream()
        .anyMatch(index -> index.getTarget().equals(column.asInternal()));
  }

  /** Check if the map/set/list column has an index on correct component that aims to filter on. */
  private boolean isIndexOnMapSetList(
      CqlIdentifier column, MapSetListFilterComponent targetFilterComponent) {

    // map the filterComponent into indexFunction first.
    var toCheckIndexFunction =
        switch (targetFilterComponent) {
          case MAP_KEY -> ApiIndexFunction.KEYS;
          case MAP_VALUE, LIST_VALUE, SET_VALUE -> ApiIndexFunction.VALUES;
          case MAP_ENTRY -> ApiIndexFunction.ENTRIES;
        };

    // there can be multiple indexes on a map column, so we need to check all of them
    return apiTableDef.indexes().allIndexesFor(column).stream()
        .anyMatch(index -> index.indexFunction() == toCheckIndexFunction);
  }

  private List<ColumnMetadata> outOfOrderClusteringKeys(
      Map<CqlIdentifier, TableFilter> identifierToFilter) {

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
                      // just need to remember we skipped one in-case we try to set another later
                      skipped[0] = true;
                      return false;
                    })
                .sorted(COLUMN_METADATA_COMPARATOR)
                .toList();

    return outOfOrderClusteringKeys;
  }

  private List<ColumnMetadata> missingPartitionKeys(
      Map<CqlIdentifier, TableFilter> identifierToFilter) {

    return tableMetadata.getPartitionKey().stream()
        .filter(column -> !identifierToFilter.containsKey(column.getName()))
        .sorted(COLUMN_METADATA_COMPARATOR)
        .toList();
  }

  /** The result of the where clause analysis. */
  public static class WhereClauseWithWarnings extends WithWarnings<WhereCQLClause<?>> {

    private final boolean requiresAllowFiltering;

    public WhereClauseWithWarnings(
        WhereCQLClause<?> target,
        List<WarningException> warningExceptions,
        boolean requiresAllowFiltering) {
      super(target, warningExceptions, null);
      this.requiresAllowFiltering = requiresAllowFiltering;
    }

    public boolean requiresAllowFiltering() {
      return requiresAllowFiltering;
    }

    @Override
    public String toString() {
      return new StringBuilder()
          .append("WhereClauseWithWarnings{")
          .append("requiresAllowFiltering=")
          .append(requiresAllowFiltering)
          .append(", warnings=")
          .append(warnings())
          .append('}')
          .toString();
    }
  }

  /** For internal use only. */
  private record AnalysisStrategy(
      List<ValidationCheckRule> validationCheckRules, List<WarningCheckRule> warningCheckRules) {}

  /** Implementations will throw if there is an issue with the filters. */
  @FunctionalInterface
  private interface ValidationCheckRule extends Consumer<Map<CqlIdentifier, TableFilter>> {}

  @FunctionalInterface
  private interface WarningCheckRule
      extends Function<Map<CqlIdentifier, TableFilter>, Optional<WarningException>> {}
}
