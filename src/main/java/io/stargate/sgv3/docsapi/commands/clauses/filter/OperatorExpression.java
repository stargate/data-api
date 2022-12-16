package io.stargate.sgv3.docsapi.commands.clauses.filter;

import io.stargate.sgv3.docsapi.commands.clauses.filter.ComparisonExpression.ComparisonMatcher;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

// We can apply more than one operation against a context node,
// e.g. {"age" : {"$gt" : 40, "$lt" : 50}}
// {"$gt" : 40, "$lt" : 50} is the expression, "$gt" : 40 is an operation
/**
 * <filter-comparison-expression> ::= <filter-comparison-path> ( <literal> |
 * <filter-operator-expression>) <filter-comparison-path> :== <document-path> THIS ->
 * <filter-operator-expression> ::= <filter-operation> (, <filter-operation>)*
 */
public class OperatorExpression {

  public List<FilterOperation> operations;

  public OperatorExpression(List<FilterOperation> operations) {
    this.operations = operations;
  }

  public static OperatorExpression from(FilterOperation... ops) {
    return new OperatorExpression(Arrays.asList(ops));
  }

  public static ComparisonMatcher<OperatorExpression> match(
      ComparisonMatcher<FilterOperation> one) {
    return new Matcher(List.of(one));
  }

  private static class Matcher implements ComparisonMatcher<OperatorExpression> {

    public List<ComparisonMatcher<FilterOperation>> operationsMatchers;

    public Matcher(List<ComparisonMatcher<FilterOperation>> operationsMatchers) {
      this.operationsMatchers = operationsMatchers;
    }

    @Override
    public Optional<JsonLiteralOrList> match(OperatorExpression t) {
      // BUG - THIS NEEDS TO test exact match, i.e. there is only on field
      ComparisonMatcher<FilterOperation> matcher = operationsMatchers.get(0);
      return matcher.match(t.operations.get(0));
    }
  }
}
