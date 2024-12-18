package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;

public interface ReadAttemptBuilder<AttemptT extends ReadAttempt<?>> {

  AttemptT build(WhereCQLClause<Select> tableWhereCQLClause);
}
