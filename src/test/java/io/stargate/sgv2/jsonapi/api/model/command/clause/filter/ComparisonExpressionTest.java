package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
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
                      ValueComparisonOperator.EQ, new JsonLiteral("abc", JsonType.STRING))),
              null);
      ComparisonExpression result = ComparisonExpression.eq("username", "abc");
      assertThat(result.getFilterOperations()).isEqualTo(expectedResult.getFilterOperations());
      assertThat(result.getPath()).isEqualTo(expectedResult.getPath());
    }

    @Test
    public void multiValueComparisonExpression() throws Exception {
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "username",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("abc", JsonType.STRING))),
              null);

      ComparisonExpression result = new ComparisonExpression("username", new ArrayList<>(), null);
      result.add(ValueComparisonOperator.EQ, "abc");
      assertThat(result.getFilterOperations()).isEqualTo(expectedResult.getFilterOperations());
      assertThat(result.getPath()).isEqualTo(expectedResult.getPath());
    }

    @Test
    public void numberValueComparisonExpression() throws Exception {
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "id",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ,
                      new JsonLiteral(BigDecimal.valueOf(10), JsonType.NUMBER))),
              null);

      ComparisonExpression result = ComparisonExpression.eq("id", BigDecimal.valueOf(10));
      assertThat(result.getFilterOperations()).isEqualTo(expectedResult.getFilterOperations());
      assertThat(result.getPath()).isEqualTo(expectedResult.getPath());
    }

    @Test
    public void booleanValueComparisonExpression() throws Exception {
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "bool",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral(true, JsonType.BOOLEAN))),
              null);

      ComparisonExpression result = ComparisonExpression.eq("bool", true);
      assertThat(result.getFilterOperations()).isEqualTo(expectedResult.getFilterOperations());
      assertThat(result.getPath()).isEqualTo(expectedResult.getPath());
    }

    @Test
    public void nullValueComparisonExpression() throws Exception {
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "nullVal",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral(null, JsonType.NULL))),
              null);

      ComparisonExpression result = ComparisonExpression.eq("nullVal", null);
      assertThat(result.getFilterOperations()).isEqualTo(expectedResult.getFilterOperations());
      assertThat(result.getPath()).isEqualTo(expectedResult.getPath());
    }

    @Test
    public void dateValueComparisonExpression() throws Exception {
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "dateVal",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral(new Date(10L), JsonType.DATE))),
              null);

      ComparisonExpression result = ComparisonExpression.eq("dateVal", new Date(10L));
      assertThat(result.getFilterOperations()).isEqualTo(expectedResult.getFilterOperations());
      assertThat(result.getPath()).isEqualTo(expectedResult.getPath());
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
                      ValueComparisonOperator.EQ, new JsonLiteral(null, JsonType.NULL))),
              null);

      List<FilterOperation<?>> match =
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
    }
  }
}
