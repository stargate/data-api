package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOption;

public class TableSelectCQLOptionsClause implements CQLOption<Select> {

  public TableSelectCQLOptionsClause() {}

  @Override
  public Select apply(Select select) {
    return select;
  }
}
