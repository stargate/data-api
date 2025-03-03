package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.service.operation.query.OrderByCqlClause;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValue;

import java.util.Objects;

/**
 * A CQL clause that adds an ORDER BY clause to a SELECT statement to ANN sort.
 *
 * <p>Note: Only supports sorting on vector columns a single column, if there is a secondary sort
 * that would be in memory sorting.
 */
public class TableOrderByANNCqlClause implements OrderByCqlClause {

  private final CqlNamedValue sortVector;
  private final Integer defaultLimit;

  public TableOrderByANNCqlClause(
      CqlNamedValue sortVector, Integer defaultLimit) {

    this.sortVector = Objects.requireNonNull(sortVector, "sortVector must not be null");
    this.defaultLimit = Objects.requireNonNull(defaultLimit, "defaultLimit must not be null");

    // sanity check
    if (sortVector.apiColumnDef().type().typeName() != ApiTypeName.VECTOR) {
      throw new IllegalArgumentException(
          "Sort vector column is not a vector, got: %s".formatted(sortVector.apiColumnDef().name().asCql(true)));
    }
  }

  @Override
  public Select apply(Select select) {

    // the named value will have run the codec and converted into the CqlVector, but it is untyped
    // (most values passed on the driver are untyped)
    // this will error if the value is deferred still, i.e. the vectorizing has not finished.
    CqlVector cqlVector = (CqlVector) sortVector.value();
    return select.orderByAnnOf(sortVector.name(), cqlVector);
  }

  @Override
  public boolean fullyCoversCommand() {
    return true;
  }

  @Override
  public Integer getDefaultLimit() {
    return defaultLimit;
  }
}
