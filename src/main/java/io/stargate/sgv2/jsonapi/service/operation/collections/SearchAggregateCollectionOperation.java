package io.stargate.sgv2.jsonapi.service.operation.collections;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.exception.DatabaseException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operation that runs OpenSearch aggregation queries against a collection.
 *
 * <p>Emits: {@code SELECT * FROM "<ks>"."<table>" WHERE expr(<saiIndexName>, ?)} where the bound
 * value is a JSON string containing an {@code "aggs"} map and an optional {@code "query"} scope.
 * HCD's {@code OpenSearchQueryHandler} detects the {@code "aggs"} key, switches to aggregation
 * mode, posts the request to OpenSearch, and returns one row with one {@code text} column per
 * top-level aggregation name. Each column value is the raw JSON result for that aggregation.
 *
 * <p>The Data API reads each column as a JSON string, parses it, and builds the {@code
 * status.aggregations} response map.
 */
public record SearchAggregateCollectionOperation(
    CommandContext<CollectionSchemaObject> commandContext,
    ObjectMapper objectMapper,
    /**
     * Serialised OpenSearch body passed as the {@code expr()} bound parameter: {@code
     * {"aggs":{...}}} or {@code {"query":{...},"aggs":{...}}}.
     */
    String exprBody)
    implements Operation<CollectionSchemaObject> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SearchAggregateCollectionOperation.class);

  /**
   * Factory method — validates that the collection has OpenSearch enabled, builds the {@code
   * expr()} body from the aggregations + optional query, and returns the operation.
   *
   * @param commandContext command context
   * @param aggregations required OpenSearch aggregation definition map
   * @param query optional OpenSearch query scope (may be {@code null} or a null JsonNode)
   * @param objectMapper mapper used to serialise the body
   * @return a ready-to-execute operation
   */
  public static SearchAggregateCollectionOperation of(
      CommandContext<CollectionSchemaObject> commandContext,
      JsonNode aggregations,
      JsonNode query,
      ObjectMapper objectMapper) {

    CollectionSchemaObject schema = commandContext.schemaObject();
    if (schema.openSearchDef() == null || !schema.openSearchDef().enabled()) {
      throw SchemaException.Code.OPEN_SEARCH_NOT_ENABLED_FOR_COLLECTION.get(errVars(schema));
    }

    // Build: { "aggs": {...} }  or  { "query": {...}, "aggs": {...} }
    ObjectNode body = objectMapper.createObjectNode();
    if (query != null && !query.isNull()) {
      body.set("query", query);
    }
    body.set("aggs", aggregations);

    String exprBody;
    try {
      exprBody = objectMapper.writeValueAsString(body);
    } catch (JsonProcessingException e) {
      // Should never happen for a well-formed JsonNode tree
      throw new RuntimeException("Failed to serialise searchAggregate body", e);
    }

    LOGGER.info(
        "[SearchAggregateCollectionOperation] built exprBody for collection={}: {}",
        schema.identifier().table(),
        exprBody);

    return new SearchAggregateCollectionOperation(commandContext, objectMapper, exprBody);
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(
      RequestContext requestContext, QueryExecutor queryExecutor) {

    CollectionSchemaObject schema = commandContext.schemaObject();
    String saiIndexName = schema.openSearchDef().saiIndexName();
    String keyspace = schema.identifier().keyspace().asInternal();
    String table = schema.identifier().table().asInternal();

    // SELECT * FROM "<ks>"."<table>" WHERE expr(<saiIndex>, ?)
    // LIMIT is deliberately omitted — HCD rejects LIMIT on aggregation queries.
    String cql =
        "SELECT * FROM \"%s\".\"%s\" WHERE expr(%s, ?)".formatted(keyspace, table, saiIndexName);

    LOGGER.info(
        "[SearchAggregateCollectionOperation] executing CQL: {} with exprBody={}", cql, exprBody);

    SimpleStatement stmt = SimpleStatement.newInstance(cql, exprBody);

    return queryExecutor
        .executeRead(requestContext, stmt, Optional.empty(), 1)
        .onItem()
        .transform(rSet -> (Supplier<CommandResult>) buildResult(rSet))
        .onFailure()
        .transform(
            failure -> {
              String msg = failure.getMessage();
              if (msg != null
                  && (msg.contains("max_agg_json_bytes")
                      || msg.contains("max_agg_response_bytes")
                      || msg.contains("aggregation") && msg.contains("exceeds"))) {
                return SchemaException.Code.OPEN_SEARCH_AGGREGATION_TOO_LARGE.get(
                    errVars(commandContext.schemaObject()));
              }
              return DatabaseException.Code.COUNT_READ_FAILED.get(
                  Map.of("errorMessage", msg != null ? msg : "unknown error"));
            });
  }

  /** Reads the one-row aggregation result and builds the result page. */
  private Supplier<CommandResult> buildResult(
      com.datastax.oss.driver.api.core.cql.AsyncResultSet rSet) {
    Row row = rSet.one();
    Map<String, JsonNode> aggregations = new LinkedHashMap<>();

    if (row != null) {
      int colCount = row.getColumnDefinitions().size();
      for (int i = 0; i < colCount; i++) {
        String colName = row.getColumnDefinitions().get(i).getName().asInternal();
        String colValue = row.getString(i);
        if (colValue != null) {
          try {
            JsonNode parsed = objectMapper.readTree(colValue);
            aggregations.put(colName, parsed);
          } catch (JsonProcessingException e) {
            LOGGER.warn(
                "[SearchAggregateCollectionOperation] failed to parse aggregation column '{}': {}",
                colName,
                e.getMessage());
            // Fall back to a plain text node so the response is still valid JSON
            aggregations.put(colName, objectMapper.getNodeFactory().textNode(colValue));
          }
        }
      }
    }

    return new SearchAggregateOperationPage(aggregations);
  }
}
