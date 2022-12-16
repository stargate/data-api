package io.stargate.sgv3.docsapi.commands.clauses.filter;

import io.stargate.sgv3.docsapi.shredding.JsonType;
import java.util.EnumSet;
import java.util.Optional;
import java.util.function.Predicate;

// Compare it to a value.
// e.g. "$gt" : 40
public class ValueComparisonOperation extends ComparisonOperation {
  public ValueComparisonOperator operator;
  // could be a list if we want to say "my_array" : {"$eq" : [1,2,3]}
  public JsonLiteralOrList rhsOperand;

  public ValueComparisonOperation(ValueComparisonOperator operator, JsonLiteralOrList rhsOperand) {
    this.operator = operator;
    this.rhsOperand = rhsOperand;
  }

  public static ComparisonExpression.ComparisonMatcher<FilterOperation> match(
      EnumSet<ValueComparisonOperator> operators, JsonType singleType) {
    return FilterOperation.wrap(
        new Matcher(ValueComparisonOperator.match(operators), JsonLiteralOrList.match(singleType)));
  }

  private static class Matcher
      implements ComparisonExpression.ComparisonMatcher<ValueComparisonOperation> {

    public Predicate<ValueComparisonOperator> operatorMatcher;
    public Predicate<JsonLiteralOrList> operandMatcher;

    public Matcher(
        Predicate<ValueComparisonOperator> operatorMatcher,
        Predicate<JsonLiteralOrList> operandMatcher) {
      this.operatorMatcher = operatorMatcher;
      this.operandMatcher = operandMatcher;
    }

    @Override
    public Optional<JsonLiteralOrList> match(ValueComparisonOperation t) {
      return (operatorMatcher.test(t.operator) && operandMatcher.test(t.rhsOperand))
          ? Optional.of(t.rhsOperand)
          : Optional.empty();
    }
  }
}
