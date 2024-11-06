package io.stargate.sgv2.jsonapi.service.operation.query;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import java.util.*;

/**
 * A collection of options that can be applied to a CQL query. Both to the object used to with the
 * query builder and the statement that is built.
 *
 * <p>{@link CQLOption} 's are keys on their class, so only one instance of each implementation can
 * be added.
 *
 * @param <QueryT> The type of the query driver class the options are applied to.
 */
public class CQLOptions<QueryT extends BuildableQuery> implements CQLClause {

  // Preserve the order we see the options, not sure if needed but prob good to keep it
  private final Map<Class<?>, CQLOption<QueryT>> builderOptions = new LinkedHashMap<>();
  private final Map<Class<?>, CQLOption<SimpleStatement>> statementOptions = new LinkedHashMap<>();

  public CQLOptions() {}

  public CQLOptions(CQLOptions<QueryT> source) {
    Objects.requireNonNull(source, "source must not be null");
    builderOptions.putAll(source.builderOptions);
    statementOptions.putAll(source.statementOptions);
  }

  public CQLOptions<QueryT> addBuilderOption(CQLOption<QueryT> option) {
    builderOptions.put(option.getClass(), option);
    return this;
  }

  public CQLOptions<QueryT> addStatementOption(CQLOption<SimpleStatement> option) {
    statementOptions.put(option.getClass(), option);
    return this;
  }

  public QueryT applyBuilderOptions(QueryT builderQuery) {
    // re-assign because the query build will return new immutable instances
    for (CQLOption<QueryT> option : builderOptions.values()) {
      builderQuery = option.apply(builderQuery);
    }
    return builderQuery;
  }

  public SimpleStatement applyStatementOptions(SimpleStatement statement) {
    // re-assign because the query build will return new immutable instances
    for (CQLOption<SimpleStatement> option : statementOptions.values()) {
      statement = option.apply(statement);
    }
    return statement;
  }
}
