package io.stargate.sgv3.docsapi.api.model.command.clause.filter;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.EnumSet;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class ComparisonExpressionTest {

  @Nested
  class ComparisonExpressionCreator {

    @Test
    public void stringValueComparisonExpression() throws Exception {
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "username",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("abc", JsonType.STRING))));

      ComparisonExpression result = ComparisonExpression.eq("username", "abc");
      assertThat(result).isEqualTo(expectedResult);
    }

    @Test
    public void numberValueComparisonExpression() throws Exception {
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "id",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ,
                      new JsonLiteral(BigDecimal.valueOf(10), JsonType.NUMBER))));

      ComparisonExpression result = ComparisonExpression.eq("id", BigDecimal.valueOf(10));
      assertThat(result).isEqualTo(expectedResult);
    }

    @Test
    public void booleanValueComparisonExpression() throws Exception {
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "bool",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral(true, JsonType.BOOLEAN))));

      ComparisonExpression result = ComparisonExpression.eq("bool", true);
      assertThat(result).isEqualTo(expectedResult);
    }

    @Test
    public void nullValueComparisonExpression() throws Exception {
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "nullVal",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral(null, JsonType.NULL))));

      ComparisonExpression result = ComparisonExpression.eq("nullVal", null);
      assertThat(result).isEqualTo(expectedResult);
    }
  }

  @Nested
  class ComparisonExpressionMatch {
    @Test
    public void matchTest() throws Exception {
      final ComparisonExpression comparisonExpression =
          new ComparisonExpression(
              "path",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral(null, JsonType.NULL))));

      List<FilterOperation> match =
          comparisonExpression.match("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.NULL);
      assertThat(match).hasSize(1);

      match =
          comparisonExpression.match("path", EnumSet.of(ValueComparisonOperator.EQ), JsonType.NULL);
      assertThat(match).hasSize(1);

      match =
          comparisonExpression.match(
              "differentPath", EnumSet.of(ValueComparisonOperator.EQ), JsonType.NULL);
      assertThat(match).hasSize(0);

      match =
          comparisonExpression.match(
              "path", EnumSet.of(ValueComparisonOperator.EQ), JsonType.STRING);
      assertThat(match).hasSize(0);

      match =
          comparisonExpression.match("path", EnumSet.of(ValueComparisonOperator.GT), JsonType.NULL);
      assertThat(match).hasSize(0);
    }
  }
}
