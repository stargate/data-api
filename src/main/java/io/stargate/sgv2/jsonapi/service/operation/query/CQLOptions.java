package io.stargate.sgv2.jsonapi.service.operation.query;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.schema.CreateIndexStart;
import java.util.*;

/**
 * A collection of options that can be applied to a CQL query. Both to the object used to with the
 * query builder and the statement that is built.
 *
 * <p>Because there is not a common base for all query builder classes, need to use sublcasses:
 *
 * <ul>
 *   <li>{@link BuildableCQLOptions} for options that can be applied to a query builder object, DML
 *       statements like select and insert.
 *   <li>{@link CreateIndexStartCQLOptions} for options for creating indexes.
 *       <p>{@link CQLOption} 's are keys on their class, so only one instance of each
 *       implementation can be added.
 *
 * @param <QueryT> The type of the query driver class the options are applied to.
 */
public abstract class CQLOptions<QueryT> implements CQLClause {

  // Preserve the order we see the options, not sure if needed but prob good to keep it
  private final Map<Class<?>, CQLOption<QueryT>> builderOptions = new LinkedHashMap<>();
  private final Map<Class<?>, CQLOption<SimpleStatement>> statementOptions = new LinkedHashMap<>();

  /** Create a new empty options object. */
  public CQLOptions() {}

  /**
   * Create a new options object with the options from the source.
   *
   * @param source The source to copy the options from.
   */
  public CQLOptions(CQLOptions<QueryT> source) {
    Objects.requireNonNull(source, "source must not be null");
    builderOptions.putAll(source.builderOptions);
    statementOptions.putAll(source.statementOptions);
  }

  /**
   * Adds the option, replacing any existing option of the same class.
   *
   * @param option The option to add.
   * @return This object.
   */
  public CQLOptions<QueryT> addBuilderOption(CQLOption<QueryT> option) {
    builderOptions.put(option.getClass(), option);
    return this;
  }

  /**
   * Adds the option, replacing any existing option of the same class.
   *
   * @param option The option to add.
   * @return This object.
   */
  public CQLOptions<QueryT> addStatementOption(CQLOption<SimpleStatement> option) {
    statementOptions.put(option.getClass(), option);
    return this;
  }

  /**
   * Apply the options to the object used with the query builder.
   *
   * @param queryBuilderTarget The object to apply the options to, it is passed to each option in
   *     order and the result of the previous option is passed to the next.
   * @return The result of the last option.
   */
  public QueryT applyBuilderOptions(QueryT queryBuilderTarget) {
    // re-assign because the query builder will return new immutable instances
    for (CQLOption<QueryT> option : builderOptions.values()) {
      queryBuilderTarget = option.apply(queryBuilderTarget);
    }
    return queryBuilderTarget;
  }

  /**
   * Apply the options to the statement.
   *
   * @param statement the statement to apply the options to, it is passed to each option in order
   *     and the result of the previous option is passed to the next.
   * @return The result of the last option.
   */
  public SimpleStatement applyStatementOptions(SimpleStatement statement) {
    // re-assign because the query build will return new immutable instances
    for (CQLOption<SimpleStatement> option : statementOptions.values()) {
      statement = option.apply(statement);
    }
    return statement;
  }

  /**
   * Options to use with a DML query such as select or insert.
   *
   * @param <BuildableT>
   */
  public static class BuildableCQLOptions<BuildableT extends BuildableQuery>
      extends CQLOptions<BuildableT> {
    public BuildableCQLOptions() {
      super();
    }

    public BuildableCQLOptions(BuildableCQLOptions<BuildableT> source) {
      super(source);
    }
  }

  /** Options to use with a create index query. */
  public static class CreateIndexStartCQLOptions extends CQLOptions<CreateIndexStart> {
    public CreateIndexStartCQLOptions() {
      super();
    }

    public CreateIndexStartCQLOptions(CreateIndexStartCQLOptions source) {
      super(source);
    }
  }
}
