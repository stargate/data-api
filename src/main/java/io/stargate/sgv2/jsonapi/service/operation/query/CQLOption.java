package io.stargate.sgv2.jsonapi.service.operation.query;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.schema.CreateIndexStart;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import java.util.function.Function;

@FunctionalInterface
public interface CQLOption<TargetT> extends Function<TargetT, TargetT>, CQLClause {

  // ===================================================================================================================
  // Options for Buildable queries
  // ===================================================================================================================

  interface ForSelect {
    static CQLOption<Select> limit(int limit) {
      return (select) -> select.limit(limit);
    }

    static CQLOption<Select> allowFiltering() {
      return Select::allowFiltering;
    }
  }

  interface ForDrop {
    static CQLOption<Drop> ifExists() {
      return Drop::ifExists;
    }
  }

  // ===================================================================================================================
  // Options for Non buildable queries
  // ===================================================================================================================

  interface ForCreateIndexStart {
    static CQLOption<CreateIndexStart> ifNotExists(boolean ifNotExists) {
      return (createIndexStart) -> ifNotExists ? createIndexStart.ifNotExists() : createIndexStart;
    }
  }

  // ===================================================================================================================
  // Options for Statements
  // ===================================================================================================================

  interface ForStatement {
    static CQLOption<SimpleStatement> pageSize(int pageSize) {
      return (statement) -> statement.setPageSize(pageSize);
    }
  }
}
