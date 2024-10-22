package io.stargate.sgv2.jsonapi.service.operation.query;

import com.datastax.oss.driver.api.querybuilder.select.Select;

import java.util.function.Function;

public interface OrderByCqlClause extends Function<Select, Select>, CQLClause {

  OrderByCqlClause NO_OP = select -> select;

}
