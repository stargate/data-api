package io.stargate.sgv2.jsonapi.service.operation.filters.table;

import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilter;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Table filter for the {@code $openSearch} operator on tables that have a {@code
 * createOpenSearchIndex} index.
 *
 * <p>Emits a CQL {@code expr(<saiIndexName>, '<dslJson>')} predicate which HCD's {@code
 * OpenSearchQueryHandler} intercepts and routes to OpenSearch. The DSL JSON is embedded inline (no
 * CQL bind marker) because the collection path does the same and HCD expects the literal form.
 *
 * <p>The {@code saiIndexName} is the Cassandra custom index identifier supplied by the caller via
 * {@code createOpenSearchIndex.name}. The {@code queryDsl} is the verbatim OpenSearch Query DSL
 * object from the {@code $openSearch} filter value — serialised to a JSON string and passed through
 * unchanged.
 */
public class OpenSearchTableFilter extends TableFilter {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenSearchTableFilter.class);

  /** Sentinel path value — this filter is not tied to a single column. */
  private static final String SEARCH_PATH = "$openSearch";

  private String saiIndexName;
  private final JsonNode queryDsl;

  /**
   * Creates a new {@code OpenSearchTableFilter} with the DSL already known. The {@code
   * saiIndexName} can be set later via {@link #setSaiIndexName(String)} once it is resolved from
   * the table schema.
   *
   * @param queryDsl the verbatim OpenSearch Query DSL supplied in the {@code $openSearch} filter
   */
  public OpenSearchTableFilter(JsonNode queryDsl) {
    // Pass null for filterBehaviour — OpenSearch filters are not "exact match" or "slice".
    super(SEARCH_PATH, null);
    this.saiIndexName = null;
    this.queryDsl = Objects.requireNonNull(queryDsl, "queryDsl must not be null");
    LOGGER.debug("OpenSearchTableFilter created (pending saiIndexName) - queryDsl={}", queryDsl);
  }

  /**
   * Convenience constructor when the SAI index name is already known.
   *
   * @param saiIndexName the Cassandra custom index identifier
   * @param queryDsl the verbatim OpenSearch Query DSL supplied in the {@code $openSearch} filter
   */
  public OpenSearchTableFilter(String saiIndexName, JsonNode queryDsl) {
    super(SEARCH_PATH, null);
    this.saiIndexName = Objects.requireNonNull(saiIndexName, "saiIndexName must not be null");
    this.queryDsl = Objects.requireNonNull(queryDsl, "queryDsl must not be null");
    LOGGER.debug(
        "OpenSearchTableFilter created - saiIndexName={}, queryDsl={}", saiIndexName, queryDsl);
  }

  /**
   * Sets the SAI index name (Cassandra custom index identifier) after the filter has been
   * constructed. Called by {@code TableReadDBOperationBuilder} after the schema is resolved.
   */
  public void setSaiIndexName(String saiIndexName) {
    this.saiIndexName = Objects.requireNonNull(saiIndexName, "saiIndexName must not be null");
  }

  public String saiIndexName() {
    return saiIndexName;
  }

  public JsonNode queryDsl() {
    return queryDsl;
  }

  /**
   * Returns a raw-CQL {@link Relation} that renders {@code expr(<saiIndexName>, '<dslJson>')}.
   *
   * <p>The DSL JSON is embedded as a literal string inside single quotes (consistent with how the
   * collection path builds the expression via {@code BuiltConditionPredicate.EXPR}). No positional
   * bind values are added.
   */
  @Override
  public Relation apply(TableSchemaObject tableSchemaObject, List<Object> positionalValues) {
    String dslJson = queryDsl.toString();
    // CQL string literals escape a single quote by doubling it
    String exprCql = "expr(" + saiIndexName + ", '" + dslJson.replace("'", "''") + "')";
    LOGGER.debug("OpenSearchTableFilter.apply() - emitting CQL fragment: {}", exprCql);
    return new RawCqlRelation(exprCql);
  }

  /** {@link Relation} implementation that renders a pre-built CQL fragment verbatim. */
  private static class RawCqlRelation implements Relation {

    private final String cql;

    RawCqlRelation(String cql) {
      this.cql = cql;
    }

    @Override
    public void appendTo(@NonNull StringBuilder builder) {
      builder.append(cql);
    }

    @Override
    public boolean isIdempotent() {
      return true;
    }
  }

  /**
   * Not used for table-path filters — the Java Driver {@link #apply} method is used instead.
   *
   * @throws UnsupportedOperationException always
   */
  @Override
  public BuiltCondition get() {
    throw new UnsupportedOperationException(
        "OpenSearchTableFilter.get() is not supported; use apply() instead");
  }

  // ── FilterBehaviour — not applicable for expr() predicates ───────────────

  @Override
  public boolean filterIsExactMatch() {
    return false;
  }

  @Override
  public boolean filterIsSlice() {
    return false;
  }
}
