package io.stargate.sgv2.jsonapi.service.operation.query;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import java.util.ArrayList;
import java.util.List;

public class CqlOptions<QueryT extends BuildableQuery> implements CQLClause {

  private final List<CQLOption<QueryT>> builderOptions = new ArrayList<>();
  private final List<CQLOption<SimpleStatement>> statementOptions = new ArrayList<>();

  public CqlOptions() {}

  public CqlOptions(CqlOptions<QueryT> copyFromCqlOptions) {
    builderOptions.addAll(copyFromCqlOptions.builderOptions);
    statementOptions.addAll(copyFromCqlOptions.statementOptions);
  }

  public CqlOptions<QueryT> addBuilderOption(CQLOption<QueryT> option) {
    builderOptions.add(option);
    return this;
  }

  public CqlOptions<QueryT> addStatementOption(CQLOption<SimpleStatement> option) {
    statementOptions.add(option);
    return this;
  }

  public BuildableQuery applyBuilderOptions(QueryT builderQuery) {
    // re-assign because the query build will return new immutable instances
    for (CQLOption<QueryT> option : builderOptions) {
      builderQuery = option.apply(builderQuery);
    }
    return builderQuery;
  }

  public SimpleStatement applyStatementOptions(SimpleStatement statement) {
    // re-assign because the query build will return new immutable instances
    for (CQLOption<SimpleStatement> option : statementOptions) {
      statement = option.apply(statement);
    }
    return statement;
  }
}
