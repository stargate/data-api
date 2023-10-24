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
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("testname", JsonType.STRING))),
              null);

      final ComparisonExpression comparisonExpression2 =
          new ComparisonExpression(
              "age",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("testage", JsonType.STRING))),
              null);

      final ComparisonExpression comparisonExpression3 =
          new ComparisonExpression(
              "_id",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("testId", JsonType.STRING))),
              null);

      LogicalExpression and = LogicalExpression.and();
      LogicalExpression nestedOr = LogicalExpression.and();
      nestedOr.addComparisonExpression(comparisonExpression2);
      nestedOr.addComparisonExpression(comparisonExpression3);
      and.addLogicalExpression(nestedOr);
      and.addComparisonExpression(comparisonExpression1);

      assertThat(and.getTotalComparisonExpressionCount()).isEqualTo(3);
      assertThat(and.getTotalIdComparisonExpressionCount()).isEqualTo(1);
    }
  }
}
