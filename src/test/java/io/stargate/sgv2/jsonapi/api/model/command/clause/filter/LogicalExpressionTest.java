package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class LogicalExpressionTest {

  @Nested
  class basicTest {
    @Test
    public void countTest() throws Exception {
      final ComparisonExpression comparisonExpression1 =
          new ComparisonExpression(
              "username",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, "testname")),
              null);

      final ComparisonExpression comparisonExpression2 =
          new ComparisonExpression(
              "age",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, "testage")),
              null);

      final ComparisonExpression comparisonExpression3 =
          new ComparisonExpression(
              "_id",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, "testId")),
              null);

      LogicalExpression and = LogicalExpression.and();
      LogicalExpression nestedOr = LogicalExpression.and();
      nestedOr.addComparisonExpressions(List.of(comparisonExpression2));
      nestedOr.addComparisonExpressions(List.of(comparisonExpression3));
      and.addLogicalExpression(nestedOr);
      and.addComparisonExpressions(List.of(comparisonExpression1));

      assertThat(and.getTotalComparisonExpressionCount()).isEqualTo(3);
      assertThat(and.getTotalIdComparisonExpressionCount()).isEqualTo(1);
    }
  }
}
