package io.stargate.sgv3.docsapi.commands.clauses.filter;

import io.stargate.sgv3.docsapi.shredding.JSONPath;
import java.util.Optional;
import java.util.function.Predicate;

public class ComparisonExpression {
  // the node in in the doc we want to test.
  public JSONPath path;

  // Spec says you can do this, compare equals to a literal {"username" : "aaron"}
  // This is a shortcut for {"username" : {"$eq" : "aaron"}}
  // In here we expand the shortcut into a canonical long form so it is all the same.
  // This means there is no "JsonLiteralOrList literal"  for this class
  public OperatorExpression operatorExpressions;

  private ComparisonExpression(JSONPath path, OperatorExpression operatorExpressions) {
    this.path = path;
    this.operatorExpressions = operatorExpressions;
  }

  /**
   * Shortcut to create equals against a literal
   *
   * <p>e.g. {"username" : "aaron"}
   *
   * @param path
   * @param value
   * @return
   */
  public static ComparisonExpression eq(String path, Object value) {
    return new ComparisonExpression(
        JSONPath.from(path),
        OperatorExpression.from(
            new ValueComparisonOperation(
                ValueComparisonOperator.EQ, JsonLiteralOrList.from(value))));
  }

  @FunctionalInterface
  public interface ComparisonMatcher<T> {
    Optional<JsonLiteralOrList> match(T t);
  }

  public static ComparisonMatcher<ComparisonExpression> match(
      String path, ComparisonMatcher<OperatorExpression> matchExpression) {
    return new Matcher(JSONPathMatcher.match(path), matchExpression);
  }

  private static class Matcher implements ComparisonMatcher<ComparisonExpression> {

    public Predicate<JSONPath> matchPath;
    public ComparisonMatcher<OperatorExpression> matchExpression;

    public Matcher(
        Predicate<JSONPath> matchPath, ComparisonMatcher<OperatorExpression> matchExpression) {
      this.matchPath = matchPath;
      this.matchExpression = matchExpression;
    }

    @Override
    public Optional<JsonLiteralOrList> match(ComparisonExpression t) {
      if (matchPath.test(t.path)) {
        return matchExpression.match(t.operatorExpressions);
      }
      return Optional.empty();
    }
  }
}
