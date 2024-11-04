package io.stargate.sgv2.jsonapi.service.operation.tables;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static io.stargate.sgv2.jsonapi.service.operation.tables.TableProjection.SIMILARITY_SCORE_ALIAS;

import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.operation.query.OrderByCqlClause;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import java.util.Objects;

/**
 * A CQL clause that adds an ORDER BY clause to a SELECT statement to ANN sort.
 *
 * <p>Note: Only supports sorting on vector columns a single column, if there is a secondary sort
 * that would be in memory sorting.
 */
public class TableOrderByANNCqlClause implements OrderByCqlClause {

  private final ApiColumnDef apiColumnDef;
  private final CqlVector<Float> vector;
  private final boolean includeSimilarity;
  private final VectorColumnDefinition vectorColumnDefinition;

  public TableOrderByANNCqlClause(
      ApiColumnDef apiColumnDef,
      CqlVector<Float> vector,
      boolean includeSimilarity,
      VectorColumnDefinition vectorColumnDefinition) {
    this.apiColumnDef = Objects.requireNonNull(apiColumnDef, "apiColumnDef must not be null");
    this.vector = Objects.requireNonNull(vector, "vector must not be null");
    this.includeSimilarity = includeSimilarity;
    this.vectorColumnDefinition =
        Objects.requireNonNull(vectorColumnDefinition, "vectorColumnDefinition must not be null");

    // sanity check
    if (apiColumnDef.type().typeName() != ApiTypeName.VECTOR) {
      throw new IllegalArgumentException(
          "ApiColumnDef must be a vector type, got: %s".formatted(apiColumnDef));
    }
  }

  @Override
  public Select apply(Select select) {

    // Apply similarity_score function if includeSimilarity is set.
    // Also, if includeSimilarity is set and NO vector sort specified, then includeSimilarity has no
    // effect.
    if (includeSimilarity) {
      select =
          select
              .function(
                  vectorColumnDefinition.similarityFunction().getFunction(),
                  Selector.column(vectorColumnDefinition.fieldName()),
                  literal(vector))
              .as(SIMILARITY_SCORE_ALIAS);
    }

    return select.orderByAnnOf(apiColumnDef.name(), vector);
  }

  @Override
  public boolean fullyCoversCommand() {
    return true;
  }
}
