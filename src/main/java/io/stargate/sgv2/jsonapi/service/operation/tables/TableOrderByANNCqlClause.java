package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.service.operation.query.OrderByCqlClause;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValueContainer;
import io.stargate.sgv2.jsonapi.service.shredding.CqlVectorNamedValue;
import io.stargate.sgv2.jsonapi.service.shredding.NamedValue;
import java.util.List;
import java.util.Objects;

/**
 * A CQL clause that adds an ORDER BY clause to a SELECT statement to ANN sort.
 *
 * <p>Note: Only supports sorting on vector columns a single column, if there is a secondary sort
 * that would be in memory sorting.
 */
public class TableOrderByANNCqlClause implements OrderByCqlClause {

  private final CqlVectorNamedValue sortVector;
  private final Integer defaultLimit;

  public TableOrderByANNCqlClause(CqlVectorNamedValue sortVector, Integer defaultLimit) {

    this.sortVector = Objects.requireNonNull(sortVector, "sortVector must not be null");
    this.defaultLimit = Objects.requireNonNull(defaultLimit, "defaultLimit must not be null");

    // sanity check
    if (sortVector.apiColumnDef().type().typeName() != ApiTypeName.VECTOR) {
      throw new IllegalArgumentException(
          "Sort vector column is not a vector, got: %s"
              .formatted(sortVector.apiColumnDef().name().asCql(true)));
    }
  }

  @Override
  public Select apply(Select select) {
    return select.orderByAnnOf(sortVector.name(), sortVector.cqlVector());
  }

  @Override
  public boolean fullyCoversCommand() {
    return true;
  }

  @Override
  public Integer getDefaultLimit() {
    return defaultLimit;
  }

  @Override
  public List<? extends NamedValue<?, ?, ?>> deferredValues() {
    return new CqlNamedValueContainer(List.of(sortVector)).deferredValues();
  }
}
