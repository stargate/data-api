package io.stargate.sgv2.jsonapi.service.operation.filters.collection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltConditionPredicate;
import io.stargate.sgv2.jsonapi.service.operation.builder.ConditionLHS;
import io.stargate.sgv2.jsonapi.service.operation.builder.JsonTerm;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter for the {@code $search} operator on OpenSearch-enabled collections.
 *
 * <p>Emits a CQL {@code expr(<saiIndexName>, '<dslJson>')} predicate which HCD's {@code
 * OpenSearchQueryHandler} intercepts and routes to OpenSearch.
 *
 * <p>The {@code saiIndexName} is the Cassandra custom index identifier (always auto-generated as
 * {@code hcd_<keyspace>_<collectionName>}). The {@code queryDsl} is the verbatim OpenSearch Query
 * DSL object supplied by the caller — it is serialised to a JSON string and passed through
 * unchanged.
 */
public class OpenSearchCollectionFilter extends CollectionFilter {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenSearchCollectionFilter.class);

  private String saiIndexName;
  private final JsonNode queryDsl;

  public OpenSearchCollectionFilter(JsonNode queryDsl) {
    this(null, queryDsl);
  }

  public OpenSearchCollectionFilter(String saiIndexName, JsonNode queryDsl) {
    super("$search");
    this.saiIndexName = saiIndexName;
    Objects.requireNonNull(queryDsl, "queryDsl must not be null");
    JsonNode rewritten = OpenSearchDslIdRewriter.rewrite(queryDsl);
    LOGGER.info(
        "[OpenSearchCollectionFilter] created - saiIndexName={}, rawQueryDsl={}, rewrittenQueryDsl={}",
        saiIndexName,
        queryDsl,
        rewritten);
    this.queryDsl = rewritten;
    this.collectionIndexUsage.openSearchIndexTag = true;
  }

  public void setSaiIndexName(String saiIndexName) {
    LOGGER.info(
        "[OpenSearchCollectionFilter] setSaiIndexName() - saiIndexName set to: {}", saiIndexName);
    this.saiIndexName = saiIndexName;
  }

  public String saiIndexName() {
    return saiIndexName;
  }

  public JsonNode queryDsl() {
    return queryDsl;
  }

  /**
   * Builds {@code expr(<saiIndexName>, ?)}.
   *
   * <p>The LHS renders as {@code expr(<saiIndexName>, } so that when {@code QueryBuilder} appends
   * the {@code EXPR} predicate (empty string) and then {@code ?}, the full expression becomes
   * {@code expr(<saiIndexName>, ?)}.
   */
  @Override
  public BuiltCondition get() {
    String dslJson = queryDsl.toString();
    LOGGER.info(
        "[OpenSearchCollectionFilter] get() - building BuiltCondition:"
            + " saiIndexName={}, dslJson={}"
            + " -> CQL will be: expr({}, ?) with bound value='{}'",
        saiIndexName,
        dslJson,
        saiIndexName,
        dslJson);
    ConditionLHS exprLhs =
        new ConditionLHS() {
          @Override
          public void appendToBuilder(StringBuilder builder) {
            builder.append("expr(").append(saiIndexName).append(", ");
          }
        };
    return BuiltCondition.of(exprLhs, BuiltConditionPredicate.EXPR, new JsonTerm(dslJson));
  }

  @Override
  protected Optional<JsonNode> jsonNodeForNewDocument(JsonNodeFactory nodeFactory) {
    return Optional.of(queryDsl);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof OpenSearchCollectionFilter other)) return false;
    return Objects.equals(saiIndexName, other.saiIndexName)
        && Objects.equals(queryDsl, other.queryDsl);
  }

  @Override
  public int hashCode() {
    return Objects.hash(saiIndexName, queryDsl);
  }
}
