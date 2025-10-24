package io.stargate.sgv2.jsonapi.service.resolver.sort;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.cql.builder.QueryBuilder;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.operation.query.OrderByCqlClause;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableOrderByANNCqlClause;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableOrderByClusteringCqlClause;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableOrderByLexicalCqlClause;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableWhereCQLClause;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.FilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.TableFilterResolver;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTableDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import io.stargate.sgv2.jsonapi.service.shredding.*;
import io.stargate.sgv2.jsonapi.service.shredding.collections.JsonPath;
import io.stargate.sgv2.jsonapi.service.shredding.tables.CqlNamedValueContainerFactory;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves a sort clause to determine if we want to apply a CQL ORDER BY clause to the operation.
 */
public class TableCqlSortClauseResolver<CmdT extends Command & Filterable & Sortable & Windowable>
    extends TableSortClauseResolver<CmdT, TableSchemaObject, OrderByCqlClause> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableCqlSortClauseResolver.class);

  private final FilterResolver<CmdT, TableSchemaObject> tableFilterResolver;

  public TableCqlSortClauseResolver(OperationsConfig operationsConfig) {
    super(operationsConfig);
    this.tableFilterResolver = new TableFilterResolver<>(operationsConfig);
  }

  /**
   * Resolve the sort clause into a {@link OrderByCqlClause}.
   *
   * @param commandContext
   * @param command
   * @return {@link OrderByCqlClause} always on null, if CQL order by cannot be used or there is no
   *     sorting the {@link OrderByCqlClause#NO_OP} is returned. Callers should check
   */
  @Override
  public WithWarnings<OrderByCqlClause> resolve(
      CommandContext<TableSchemaObject> commandContext, CmdT command) {
    Objects.requireNonNull(commandContext, "commandContext is required");
    Objects.requireNonNull(command, "command is required");

    var sortClause = command.sortClause(commandContext);
    if (sortClause == null || sortClause.isEmpty()) {
      LOGGER.debug("Sort clause is null or empty, no CQL ORDER BY needed.");
      return WithWarnings.of(OrderByCqlClause.NO_OP);
    }

    // NOTE: existence of the columns is checked in the SortClauseBuilder, no need to re-check

    // First: Lexical sort? Must be alone, if it exists (already validated)
    var lexicalSortExpr = sortClause.lexicalSortExpression();
    if (lexicalSortExpr != null) {
      return resolveLexicalSort(commandContext, lexicalSortExpr, command.skip(), command.limit());
    }

    var vectorAndVectorizeSorts = sortClause.tableVectorSorts();
    if (vectorAndVectorizeSorts.isEmpty()) {
      var whereCQLClause =
          TableWhereCQLClause.forSelect(
              commandContext.schemaObject(), tableFilterResolver.resolve(commandContext, command));
      return resolveNonVectorSort(
          commandContext,
          whereCQLClause.target(),
          sortClause,
          sortClause.sortColumnIdentifiers(),
          command.skip());
    }
    return resolveVectorSort(
        commandContext, sortClause, vectorAndVectorizeSorts, command.skip(), command.limit());
  }

  /**
   * We have at least one sort expression, and none of them are vector sorts.
   *
   * <p>If the sort uses the clustering keys in the correct way according to CQL, then we can use
   * the CQL Order By to push the sorting to the database. See {@link
   * TableOrderByClusteringCqlClause}. If not then we return a {@link OrderByCqlClause#NO_OP} to
   * indicate that we cannot push the sorting to the database and need to do it in memory.
   */
  private WithWarnings<OrderByCqlClause> resolveNonVectorSort(
      CommandContext<TableSchemaObject> commandContext,
      WhereCQLClause<Select> whereCQLClause,
      SortClause sortClause,
      List<CqlIdentifier> sortColumns,
      Optional<Integer> skip) {

    var apiTableDef = commandContext.schemaObject().apiTableDef();
    if (skip.isPresent()) {
      var warn = WarningException.Code.IN_MEMORY_SORTING_DUE_SKIP_OPTIONS.get();
      return WithWarnings.of(OrderByCqlClause.NO_OP, warn);
    }
    // If there is any sorting on non partition sorting columns, we cannot use CQL ORDER BY
    var nonClusteringKeySorts =
        sortColumns.stream()
            .filter(sortColumn -> !apiTableDef.clusteringKeys().containsKey(sortColumn))
            .sorted(CQL_IDENTIFIER_COMPARATOR)
            .toList();

    if (!nonClusteringKeySorts.isEmpty()) {
      var warn =
          WarningException.Code.IN_MEMORY_SORTING_DUE_TO_NON_PARTITION_SORTING.get(
              errVars(
                  commandContext.schemaObject(),
                  map -> {
                    map.put(
                        "partitionSorting",
                        errFmtApiColumnDef(apiTableDef.clusteringKeys().values()));
                    map.put("sortColumns", errFmtCqlIdentifier(nonClusteringKeySorts));
                  }));
      return WithWarnings.of(OrderByCqlClause.NO_OP, warn);
    }

    // We know all the order keys are partition sorting keys
    // if the order of the sort columns is not the same as the clustering keys, we cannot use CQL
    // this covers two cases: if the clustering is [a,b,c] covers
    // -  [a,c] where b is missed
    // - [a,c,b] where the order is wrong
    boolean isOutOfOrder = false;
    var clusteringKeys = apiTableDef.clusteringKeys().identifiers();
    for (int i = 0; i < sortColumns.size(); i++) {
      if (!sortColumns.get(i).equals(clusteringKeys.get(i))) {
        isOutOfOrder = true;
        break;
      }
    }
    if (isOutOfOrder) {
      var warn =
          WarningException.Code.IN_MEMORY_SORTING_DUE_TO_OUT_OF_ORDER_PARTITION_SORTING.get(
              errVars(
                  commandContext.schemaObject(),
                  map -> {
                    map.put(
                        "partitionSorting",
                        errFmtApiColumnDef(apiTableDef.clusteringKeys().values()));
                    map.put(
                        "sortColumns",
                        errFmtCqlIdentifier(sortColumns)); // no not change the order of the sorting
                  }));

      return WithWarnings.of(OrderByCqlClause.NO_OP, warn);
    }

    if (!whereCQLClause.selectsSinglePartition(commandContext.schemaObject())) {
      var warn =
          WarningException.Code.IN_MEMORY_SORTING_DUE_TO_PARTITION_KEY_NOT_RESTRICTED.get(
              errVars(
                  commandContext.schemaObject(),
                  map -> {
                    map.put("partitionKeys", errFmtApiColumnDef(apiTableDef.partitionKeys()));
                    map.put("partitionSorting", errFmtApiColumnDef(apiTableDef.clusteringKeys()));
                    map.put("sortColumns", errFmtCqlIdentifier(sortColumns));
                  }));

      return WithWarnings.of(OrderByCqlClause.NO_OP, warn);
    }

    var orderByTerms =
        sortClause.sortExpressions().stream()
            .map(
                sortExpression ->
                    new TableOrderByClusteringCqlClause.OrderByTerm(
                        apiTableDef.allColumns().get(sortExpression.pathAsCqlIdentifier()),
                        sortExpression.isAscending()
                            ? TableOrderByClusteringCqlClause.Order.ASC
                            : TableOrderByClusteringCqlClause.Order.DESC))
            .toList();
    var cqlOrderBy = new TableOrderByClusteringCqlClause(orderByTerms);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Sort covered by clustering keys, using CQL ORDER BY. cqlOrderBy: {}", cqlOrderBy);
    }
    return WithWarnings.of(cqlOrderBy);
  }

  /**
   * We have (only) lexical sort in the sort clause.
   *
   * <p>This is always implemented by using a CQL Order By BM25 OF to push the search to the
   * database. See {@link TableOrderByLexicalCqlClause}
   */
  private WithWarnings<OrderByCqlClause> resolveLexicalSort(
      CommandContext<TableSchemaObject> commandContext,
      SortExpression lexicalSortExpr,
      Optional<Integer> skip,
      Optional<Integer> limit) {
    if (skip.isPresent()) {
      throw SortException.Code.CANNOT_LEXICAL_SORT_WITH_SKIP_OPTION.get();
    }
    Integer actualLimit = limit.orElse(QueryBuilder.DEFAULT_BM25_LIMIT);
    actualLimit = Math.min(actualLimit, QueryBuilder.MAX_BM25_LIMIT);

    final ApiTableDef apiTableDef = commandContext.schemaObject().apiTableDef();
    final CqlIdentifier lexicalSortIdentifier = lexicalSortExpr.pathAsCqlIdentifier();
    final ApiColumnDef lexicalSortColumn = apiTableDef.allColumns().get(lexicalSortIdentifier);

    // Column type already validated, no need to check again
    // But we do want to check it has lexical index.
    var maybeIndex = apiTableDef.indexes().firstIndexFor(lexicalSortIdentifier);
    if (maybeIndex.isEmpty()) {
      throw SortException.Code.CANNOT_LEXICAL_SORT_NON_INDEXED_COLUMNS.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put(
                    "indexedColumns", errFmtJoin(indexedColumns(commandContext.schemaObject())));
                map.put("sortColumn", errFmt(lexicalSortIdentifier));
              }));
    }

    // HACK - Tatu, 08-Jul-2025 - copied from Aaron's hack for Vector search
    // (the better solution would be to parse the sort JSON node through the JsonNamedValueFactory
    // with a
    // decoder that understands sorting, but the sort clause is terrible and needs to be refactored)

    var jsonNamedValue =
        new JsonNamedValue(JsonPath.from(lexicalSortExpr.getPath()), JsonNodeDecoder.DEFAULT);
    if (jsonNamedValue.bind(commandContext.schemaObject())) {
      // ok, this is a terrible hack, but it needs a JSON node
      jsonNamedValue.prepare(JsonNodeFactory.instance.textNode(lexicalSortExpr.getLexicalQuery()));
    } else {
      throw new IllegalStateException(
          "jsonNamedValue failed to bind for the sorting on column " + lexicalSortColumn.name());
    }

    var lexicalNamedValue =
        new CqlNamedValueContainerFactory(
                CqlNamedValue::new,
                commandContext.schemaObject(),
                JSONCodecRegistries.DEFAULT_REGISTRY,
                SORTING_NAMED_VALUE_ERROR_STRATEGY)
            .create(new JsonNamedValueContainer(List.of(jsonNamedValue))).values().stream()
                .findFirst()
                .get();

    return WithWarnings.of(new TableOrderByLexicalCqlClause(lexicalNamedValue, actualLimit));
  }

  /**
   * We have at least one vector sort in the sort clause.
   *
   * <p>This is always implemented by using a CQL Order By to push the ANN search to the database.
   * See {@link TableOrderByANNCqlClause}
   */
  private WithWarnings<OrderByCqlClause> resolveVectorSort(
      CommandContext<TableSchemaObject> commandContext,
      SortClause sortClause,
      List<SortExpression> vectorAndVectorizeSorts,
      Optional<Integer> skip,
      Optional<Integer> limit) {

    // we are getting both vector and vectorize sorts, when we bind and prepare the value
    // vectorize will be used if needed, via the Deferrable interface.
    // work out which here, useful later.
    boolean isVectorize =
        vectorAndVectorizeSorts.stream().anyMatch(SortExpression::isTableVectorizeSort);

    if (limit.isPresent()
        && limit.get()
            > commandContext.config().get(OperationsConfig.class).maxVectorSearchLimit()) {
      throw SortException.Code.CANNOT_VECTOR_SORT_WITH_LIMIT_EXCEEDS_MAX.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put(
                    "sortColumn",
                    errFmtJoin(
                        vectorAndVectorizeSorts.stream().map(SortExpression::getPath).toList()));
                map.put("limit", String.valueOf(limit.get()));
                map.put(
                    "maxLimit",
                    String.valueOf(
                        commandContext
                            .config()
                            .get(OperationsConfig.class)
                            .maxVectorSearchLimit()));
              }));
    }

    if (skip.isPresent()) {
      throw SortException.Code.CANNOT_VECTOR_SORT_WITH_SKIP_OPTION.get();
    }

    var apiTableDef = commandContext.schemaObject().apiTableDef();
    if (vectorAndVectorizeSorts.size() > 1) {
      var errorCode =
          isVectorize
              ? SortException.Code.CANNOT_SORT_ON_MULTIPLE_VECTORIZE
              : SortException.Code.CANNOT_SORT_ON_MULTIPLE_VECTORS;
      throw errorCode.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put(
                    "vectorColumns",
                    errFmtApiColumnDef(apiTableDef.allColumns().filterVectorColumnsToList()));
                map.put(
                    "sortColumns",
                    errFmtJoin(
                        vectorAndVectorizeSorts.stream().map(SortExpression::getPath).toList()));
              }));
    }

    // we have one vector sort - cannot have any other sorting
    var nonVectorSorts = sortClause.nonTableVectorSorts();
    if (!nonVectorSorts.isEmpty()) {
      throw SortException.Code.CANNOT_SORT_VECTOR_AND_NON_VECTOR_COLUMNS.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put(
                    "vectorColumns",
                    errFmtApiColumnDef(apiTableDef.allColumns().filterVectorColumnsToList()));
                map.put(
                    "sortVectorColumns",
                    errFmtJoin(
                        vectorAndVectorizeSorts.stream().map(SortExpression::getPath).toList()));
                map.put(
                    "sortNonVectorColumns",
                    errFmtJoin(nonVectorSorts.stream().map(SortExpression::getPath).toList()));
              }));
    }

    var vectorSortExpression = vectorAndVectorizeSorts.getFirst();
    var vectorSortIdentifier = vectorSortExpression.pathAsCqlIdentifier();
    var vectorSortColumn = apiTableDef.allColumns().get(vectorSortIdentifier);

    if (vectorSortColumn.type().typeName() != ApiTypeName.VECTOR) {
      var errorCode =
          isVectorize
              ? SortException.Code.CANNOT_VECTORIZE_SORT_NON_VECTOR_COLUMN
              : SortException.Code.CANNOT_VECTOR_SORT_NON_VECTOR_COLUMNS;
      throw errorCode.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put(
                    "vectorColumns",
                    errFmtApiColumnDef(apiTableDef.allColumns().filterVectorColumnsToList()));
                map.put("sortColumns", errFmt(vectorSortIdentifier));
              }));
    }

    // already validated the column is a vector type
    // the optional get won't fail
    var vectorColumnDefinition =
        commandContext
            .schemaObject()
            .vectorConfig()
            .getColumnDefinition(vectorSortIdentifier)
            .get();

    // see if Table has vector index on the target sort vector column
    var optionalVectorIndex = apiTableDef.indexes().firstIndexFor(vectorSortIdentifier);
    if (optionalVectorIndex.isEmpty()) {
      throw SortException.Code.CANNOT_VECTOR_SORT_NON_INDEXED_VECTOR_COLUMNS.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put(
                    "vectorColumns",
                    errFmtApiColumnDef(apiTableDef.allColumns().filterVectorColumnsToList()));
                map.put(
                    "indexedColumns",
                    errFmtJoin(indexedVectorColumns(commandContext.schemaObject())));
                map.put("sortColumns", errFmt(vectorSortIdentifier));
              }));
    }

    // This is a bit of a hack, we should be using the codecs to convert but for now the Sort
    // deserialization
    // turns the JSON array into a float array, so we can just use that.
    // Needs more refactoring to change how it works
    LOGGER.debug(
        "Vector sorting on column {}", cqlIdentifierToMessageString(vectorSortColumn.name()));

    // HACK - Aaron 3 march 2025 - this is a hack to get the sort into the NamedValue model
    // the better solution would be to parse the sort JSON node through the JsonNamedValueFactory
    // with a
    // decoder that understands sorting, but the sort clause is terrible and needs to be refactored

    var jsonNamedValue =
        new JsonNamedValue(JsonPath.from(vectorSortExpression.getPath()), JsonNodeDecoder.DEFAULT);
    if (jsonNamedValue.bind(commandContext.schemaObject())) {
      // ok, this is a terrible hack, but it needs a JSON node
      JsonNode jsonNode;
      if (vectorSortExpression.hasVectorize()) {
        jsonNode = JsonNodeFactory.instance.textNode(vectorSortExpression.getVectorize());
      } else if (vectorSortExpression.hasVector()) {

        // check if provided vector dimension matches column definition
        if (vectorSortExpression.getVector().length != vectorColumnDefinition.vectorSize()) {
          throw SortException.Code.CANNOT_VECTOR_SORT_ON_MISMATCHED_VECTOR_DIMENSIONS.get(
              errVars(
                  commandContext.schemaObject(),
                  map -> {
                    map.put(
                        "vectorColumns",
                        errFmtApiColumnDef(apiTableDef.allColumns().filterVectorColumnsToList()));
                    map.put("targetVectorColumn", errFmt(vectorSortIdentifier));
                    map.put("actualDimension", String.valueOf(vectorColumnDefinition.vectorSize()));
                    map.put(
                        "providedDimension",
                        String.valueOf(vectorSortExpression.getVector().length));
                  }));
        }

        var arrayNode = JsonNodeFactory.instance.arrayNode();
        for (float f : vectorSortExpression.getVector()) {
          arrayNode.add(f);
        }
        jsonNode = arrayNode;
      } else {
        throw new IllegalStateException("vectorSortExpression has no vector or vectorize value");
      }
      jsonNamedValue.prepare(jsonNode);
    } else {
      throw new IllegalStateException(
          "jsonNamedValue failed to bind for the sorting on column " + vectorSortColumn.name());
    }

    // will throw is there is an error
    // There will be a single value, and we know it is a vector, so use the overload to
    // create a CqlVectorNamedValue, see class docs for why
    var vectorNamedValue =
        new CqlNamedValueContainerFactory(
                CqlVectorNamedValue::new,
                commandContext.schemaObject(),
                JSONCodecRegistries.DEFAULT_REGISTRY,
                SORTING_NAMED_VALUE_ERROR_STRATEGY)
            .create(new JsonNamedValueContainer(List.of(jsonNamedValue))).values().stream()
                .findFirst()
                .map(namedValue -> (CqlVectorNamedValue) namedValue)
                .get();

    return WithWarnings.of(
        new TableOrderByANNCqlClause(
            vectorNamedValue,
            commandContext.config().get(OperationsConfig.class).maxVectorSearchLimit()),
        List.of(WarningException.Code.ZERO_FILTER_OPERATIONS));
  }

  private Optional<IndexMetadata> findIndexMetadata(
      TableSchemaObject schemaObject, ApiColumnDef targetColumn) {
    return schemaObject.tableMetadata().getIndexes().values().stream()
        .filter(index -> index.getTarget().equals(targetColumn.name().asInternal()))
        .findFirst();
  }

  private List<String> indexedColumns(TableSchemaObject schemaObject) {
    var columns = schemaObject.apiTableDef().allColumns();
    return schemaObject.tableMetadata().getIndexes().values().stream()
        .map(IndexMetadata::getTarget)
        .filter(target -> columns.containsKey(CqlIdentifier.fromInternal(target)))
        .sorted()
        .toList();
  }

  private List<String> indexedVectorColumns(TableSchemaObject schemaObject) {
    var apiVectorColumns = schemaObject.apiTableDef().allColumns().filterVectorColumnsToContainer();
    return schemaObject.tableMetadata().getIndexes().values().stream()
        .map(IndexMetadata::getTarget)
        .filter(target -> apiVectorColumns.containsKey(CqlIdentifier.fromInternal(target)))
        .toList();
  }

  /**
   * HACK - for now we are not using the error checking in the cql named value, all the checks are
   * in the sort resolver. But we need to rely on this for checking if vectorize is enabled on the
   * column
   */
  private static final CqlNamedValue.ErrorStrategy<SortException>
      SORTING_NAMED_VALUE_ERROR_STRATEGY =
          new CqlNamedValue.ErrorStrategy<>() {

            @Override
            public ErrorCode<SortException> codeForUnknownColumn() {
              throw new UnsupportedOperationException();
            }

            @Override
            public ErrorCode<SortException> codeForMissingCodec() {
              throw new UnsupportedOperationException();
            }

            @Override
            public ErrorCode<SortException> codeForMissingVectorize() {
              return SortException.Code.CANNOT_VECTORIZE_SORT_WHEN_MISSING_VECTORIZE_DEFINITION;
            }

            @Override
            public ErrorCode<SortException> codeForCodecError() {
              throw new UnsupportedOperationException();
            }

            @Override
            public void allChecks(
                TableSchemaObject tableSchemaObject, CqlNamedValueContainer allColumns) {
              checkMissingVectorize(tableSchemaObject, allColumns);
              checkMultipleSortVectorize(tableSchemaObject, allColumns);
            }

            private void checkMultipleSortVectorize(
                TableSchemaObject tableSchemaObject, CqlNamedValueContainer allColumns) {
              if (allColumns.size() > 1) {
                var sorted =
                    allColumns.values().stream().sorted(CqlNamedValue.NAME_COMPARATOR).toList();

                throw SortException.Code.CANNOT_SORT_ON_MULTIPLE_VECTORIZE.get(
                    errVars(
                        tableSchemaObject,
                        map -> {
                          map.put("sortVectorizeColumns", errFmtCqlNamedValue(sorted));
                        }));
              }
            }
          };
}
