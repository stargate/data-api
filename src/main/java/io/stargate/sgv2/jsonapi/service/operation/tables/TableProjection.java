package io.stargate.sgv2.jsonapi.service.operation.tables;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_FUNCTION_SIMILARITY_FIELD;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtApiColumnDef;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.querybuilder.select.OngoingSelection;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnsDescContainer;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.checked.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.ToJSONCodecException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.operation.OperationProjection;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.*;
import io.stargate.sgv2.jsonapi.service.operation.query.SelectCQLClause;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.util.CqlVectorUtil;
import java.util.*;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The projection for a read operation.
 *
 * <p>This class does double duty: it implements the {@link SelectCQLClause} interface to apply the
 * projection to the cql statement, and it implements the {@link OperationProjection} interface to
 * apply the projection to rows read from the database.
 *
 * <p>TODO: refactor to use the factory / builder pattern for other clauses and give better errors
 */
public class TableProjection implements SelectCQLClause, OperationProjection {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableProjection.class);

  private ObjectMapper objectMapper;
  private TableSchemaObject table;
  private List<ColumnMetadata> columns;
  private ColumnsDescContainer columnsDesc;
  private SimilarityScoreFunction similarityScoreFunction;

  private TableProjection(
      ObjectMapper objectMapper,
      TableSchemaObject table,
      List<ColumnMetadata> columns,
      ColumnsDescContainer columnsDesc,
      SimilarityScoreFunction similarityScoreFunction) {
    this.objectMapper = objectMapper;
    this.table = table;
    this.columns = columns;
    this.columnsDesc = columnsDesc;
    this.similarityScoreFunction = similarityScoreFunction;
  }

  /**
   * Factory method for construction projection instance, given a projection definition and table
   * schema.
   */
  public static <CmdT extends Projectable & VectorSortable> TableProjection fromDefinition(
      ObjectMapper objectMapper, CmdT command, TableSchemaObject table) {

    Map<String, ColumnMetadata> columnsByName = new HashMap<>();
    // TODO: This can also be cached as part of TableSchemaObject than resolving it for every query.
    table
        .tableMetadata()
        .getColumns()
        .forEach((id, column) -> columnsByName.put(id.asInternal(), column));

    List<ColumnMetadata> columns =
        command.tableProjectionDefinition().extractSelectedColumns(columnsByName);

    // TODO: A table can't be with empty columns. Think a redundant check.
    if (columns.isEmpty()) {
      throw ErrorCodeV1.UNSUPPORTED_PROJECTION_DEFINITION.toApiException(
          "did not include any Table columns");
    }

    // result set has ColumnDefinitions not ColumnMetadata kind of weird

    var readApiColumns =
        table
            .apiTableDef()
            .allColumns()
            .filterBy(columns.stream().map(ColumnMetadata::getName).toList());
    if (!readApiColumns.filterByUnsupported().isEmpty()) {
      throw new IllegalStateException(
          "Unsupported columns in the result set: %s"
              .formatted(errFmtApiColumnDef(readApiColumns.filterByUnsupported())));
    }

    return new TableProjection(
        objectMapper,
        table,
        columns,
        readApiColumns.toColumnsDesc(),
        SimilarityScoreFunction.from(command, table));
  }

  @Override
  public Select apply(OngoingSelection ongoingSelection) {
    Set<CqlIdentifier> readColumns = new LinkedHashSet<>();
    readColumns.addAll(columns.stream().map(ColumnMetadata::getName).toList());
    Select select = ongoingSelection.columnsIds(readColumns);

    // may apply similarity score function
    return similarityScoreFunction.apply(select);
  }

  @Override
  public JsonNode projectRow(Row row) {
    long startNano = System.nanoTime();
    int nonNullCount = 0;
    int skippedNullCount = 0;

    ObjectNode result = objectMapper.createObjectNode();
    for (int i = 0, len = columns.size(); i < len; ++i) {
      final ColumnMetadata column = columns.get(i);
      final String columnName = column.getName().asInternal();
      JSONCodec codec;

      // TODO: maybe optimize common case of String, Boolean to avoid conversions, lookups
      try {
        codec = JSONCodecRegistries.DEFAULT_REGISTRY.codecToJSON(table.tableMetadata(), column);
      } catch (MissingJSONCodecException e) {
        throw ErrorCodeV1.UNSUPPORTED_PROJECTION_PARAM.toApiException(
            "Column '%s' has unsupported type '%s'", columnName, column.getType().toString());
      }
      try {
        final Object columnValue = row.getObject(i);
        // By default, null value will not be returned.
        // https://github.com/stargate/data-api/issues/1636 issue for adding nullOption
        if (columnValue == null) {
          skippedNullCount++;
        } else {
          nonNullCount++;
          result.put(columnName, codec.toJSON(objectMapper, columnValue));
        }

      } catch (ToJSONCodecException e) {
        throw ErrorCodeV1.UNSUPPORTED_PROJECTION_PARAM.toApiException(
            e,
            "Column '%s' has invalid value of type '%s': failed to convert to JSON: %s",
            columnName,
            column.getType().toString(),
            e.getMessage());
      }
    }

    if (LOGGER.isDebugEnabled()) {
      double durationMs = (System.nanoTime() - startNano) / 1_000_000.0;
      LOGGER.debug(
          "projectRow() row build durationMs={}, columns.size={}, nonNullCount={}, skippedNullCount={}",
          durationMs,
          columns.size(),
          nonNullCount,
          skippedNullCount);
    }

    // If user specify includeSimilarity, but no ANN sort clause, then we won't generate
    // similarity_score function in the cql statement
    if (similarityScoreFunction.needProjection()) {
      try {
        final float aFloat = row.getFloat(SimilarityScoreFunction.SIMILARITY_SCORE_ALIAS);
        result.put(VECTOR_FUNCTION_SIMILARITY_FIELD, aFloat);
        // Should not happen, but keep it caught, in case it breaks the query
      } catch (IllegalArgumentException ignored) {
      }
    }

    return result;
  }

  @Override
  public ColumnsDescContainer getSchemaDescription() {
    return columnsDesc;
  }

  private record SimilarityScoreFunction(
      boolean requestedSimilarityScore,
      CqlIdentifier requestedVectorColumnPath,
      CqlVector<Float> vector,
      String function)
      implements Function<Select, Select> {

    private static final SimilarityScoreFunction NO_OP =
        new SimilarityScoreFunction(false, null, null, null);

    // Make a unique constant string as similarity score function alias in cql statement
    // E.G. SELECT id,similarity_euclidean(vector_type,[0.2, 0.15, 0.3]) AS
    // similarityScore1699123456789 from xxx;
    private static final String SIMILARITY_SCORE_ALIAS =
        "similarityScore" + System.currentTimeMillis();

    static <CmdT extends VectorSortable> SimilarityScoreFunction from(
        CmdT command, TableSchemaObject table) {
      // SimilarityScore is only included when
      // 1. tableSchemaObject has vector enabled
      // 2. includeSimilarityScore is set
      // 3. there is a vector sort clause
      var requestedSimilarityScore = command.includeSimilarityScore().orElse(false);
      var sortExpression = command.sortExpression();
      var requestedVectorColumnPath =
          sortExpression.map(SortExpression::pathAsCqlIdentifier).orElse(null);
      var requestedVector = sortExpression.map(SortExpression::vector).orElse(null);

      String similarityFunctionFromVectorConfig = null;
      if (requestedVectorColumnPath != null) {
        similarityFunctionFromVectorConfig =
            table
                .vectorConfig()
                .getColumnDefinition(requestedVectorColumnPath)
                .map(VectorColumnDefinition::similarityFunction)
                .map(SimilarityFunction::getFunction)
                .orElse(null);
      }

      if (!requestedSimilarityScore
          || requestedVectorColumnPath == null
          || requestedVector == null
          || similarityFunctionFromVectorConfig == null) {
        return NO_OP;
      }
      return new SimilarityScoreFunction(
          true,
          requestedVectorColumnPath,
          CqlVectorUtil.floatsToCqlVector(requestedVector),
          similarityFunctionFromVectorConfig);
    }

    @Override
    public Select apply(Select select) {
      if (this == NO_OP) {
        return select;
      }

      return select
          .function(function, Selector.column(requestedVectorColumnPath), literal(vector))
          .as(SIMILARITY_SCORE_ALIAS);
    }

    private boolean needProjection() {
      return this != NO_OP;
    }
  }
}
