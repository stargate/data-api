package io.stargate.sgv2.jsonapi.service.operation.query;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A collection of options that can be applied to a CQL query, either during the building phase using
 * {@link #applyBuilderOptions(Object)} or after the statement has been created using {
 * @link #applyStatementOptions(SimpleStatement)}.
 * <p>
 * Use either of the two subclasses:
 * <ul>
 *   <li>{@link BindableQueryOptions} for queries that can be bound to values, these are the DML queries like select
 *   and insert.</li>
 *   <li>{@link NonBindableQueryOptions} for queries that cannot be bound to values, these are the DDL queries like
 *   create table and create index.</li>
 * </ul>
 * @param <QueryT> The class used from the driver to build the query.
 */
public abstract class CqlOptions<QueryT> implements CQLClause {

  private final List<CQLOption<QueryT>> builderOptions = new ArrayList<>();
  private final List<CQLOption<SimpleStatement>> statementOptions = new ArrayList<>();

  /**
   * Create an empty set of options.
   */
  public CqlOptions() {}

  /**
   * Create a new set of options with the same options as the source.
   * @param source The source to copy the options from.
   */
  public CqlOptions(CqlOptions<QueryT> source) {
    Objects.requireNonNull(source, "source must not be null");
    builderOptions.addAll(source.builderOptions);
    statementOptions.addAll(source.statementOptions);
  }

  /**
   * Adds the {@link CQLOption} to the list of options that will be applied to the query during the building phase
   * @param option
   * @return
   */
  public CqlOptions<QueryT> addBuilderOption(CQLOption<QueryT> option) {
    builderOptions.add(option);
    return this;
  }

  public CqlOptions<QueryT> addStatementOption(CQLOption<SimpleStatement> option) {
    statementOptions.add(option);
    return this;
  }

  public QueryT applyBuilderOptions(QueryT builderQuery) {
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

  public static class BindableQueryOptions<T extends BuildableQuery> extends CqlOptions<T> {
    public BindableQueryOptions() {}

    public BindableQueryOptions(CqlOptions<T> source) {
      super(source);
    }
  }

  public static class NonBindableQueryOptions<T> extends CqlOptions<T> {
    public NonBindableQueryOptions() {}

    public NonBindableQueryOptions(CqlOptions<T> source) {
      super(source);
    }
  }
}
