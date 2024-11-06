package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.querybuilder.select.Select;
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

  public TableOrderByANNCqlClause(ApiColumnDef apiColumnDef, CqlVector<Float> vector) {
    this.apiColumnDef = Objects.requireNonNull(apiColumnDef, "apiColumnDef must not be null");
    this.vector = Objects.requireNonNull(vector, "vector must not be null");

    // sanity check
    if (apiColumnDef.type().typeName() != ApiTypeName.VECTOR) {
      throw new IllegalArgumentException(
          "ApiColumnDef must be a vector type, got: %s".formatted(apiColumnDef));
    }
  }

  @Override
  public Select apply(Select select) {
    return select.orderByAnnOf(apiColumnDef.name(), vector);
  }

  @Override
  public boolean fullyCoversCommand() {
    return true;
  }
}
