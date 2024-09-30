package io.stargate.sgv2.jsonapi.service.operation.query;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import java.util.function.Function;

@FunctionalInterface
public interface CQLOption<TargetT> extends Function<TargetT, TargetT>, CQLClause {

  interface ForSelect {
    static CQLOption<Select> limit(int limit) {
      return (select) -> select.limit(limit);
    }

    static CQLOption<Select> withAllowFiltering() {
      return Select::allowFiltering;
    }
  }

  interface ForUpdate {
    static CQLOption<Select> limit(int limit) {
      return (select) -> select.limit(limit);
    }
  }

  interface ForStatement {
    static CQLOption<SimpleStatement> pageSize(int pageSize) {
      return (statement) -> statement.setPageSize(pageSize);
    }
  }
}
