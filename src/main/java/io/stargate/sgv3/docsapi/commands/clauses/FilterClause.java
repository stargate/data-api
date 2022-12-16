package io.stargate.sgv3.docsapi.commands.clauses;

import io.stargate.sgv3.docsapi.commands.clauses.filter.ComparisonExpression;
import io.stargate.sgv3.docsapi.commands.clauses.filter.LogicalExpression;
import java.util.ArrayList;
import java.util.List;

/**
 * The filter clause in find and some modify commands.
 *
 * <p>The attempt to model a filter clause below is just an attempt to see how complex it is. I
 * think we should model the clause as best we can according to the spec, and then work out how we
 * will translate from JSON into the model. It will not be easy no matter what.
 */
public class FilterClause extends Clause {

  // These are the top level of the filter clause
  // All the expressions must be True, they are AND'd together implicitly.
  public List<ComparisonExpression> comparisonExpressions = new ArrayList<>();
  public List<LogicalExpression> logicalExpressions = new ArrayList<>();

  public FilterClause() {}

  public interface Filterable {
    FilterClause getFilter();
  }

  // =================================================
  // ============ Fluent API below ===================
  // =================================================

  /**
   * @param path
   * @param literal
   * @return
   */
  public FilterClause eq(String path, Object value) {
    comparisonExpressions.add(ComparisonExpression.eq(path, value));
    return this;
  }

  // =================================================
  // ============ Classes and Types below ============
  // =================================================

  // This is here so a logical expression can include everything in it.
  public static class FilterExpression {
    // All the expressions must be True, they are AND'd together implicitly.
    public final List<ComparisonExpression> comparisonExpressions = new ArrayList<>();
    public final List<LogicalExpression> logicalExpressions = new ArrayList<>();
  }
}
