package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.jsonapi.api.model.command.table.MapSetListComponent;
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
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, "abc")),
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
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, "abc")),
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
                  ValueComparisonOperation.build(
                      ValueComparisonOperator.EQ, BigDecimal.valueOf(10))),
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
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, true)),
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
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, null)),
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
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, new Date(10L))),
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
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, null)),
              null);

      List<FilterOperation<?>> match =
          comparisonExpression.match(
              "*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.NULL, false);
      assertThat(match).hasSize(1);

      match =
          comparisonExpression.match(
              "path", EnumSet.of(ValueComparisonOperator.EQ), JsonType.NULL, false);
      assertThat(match).hasSize(1);

      match =
          comparisonExpression.match(
              "differentPath", EnumSet.of(ValueComparisonOperator.EQ), JsonType.NULL, false);
      assertThat(match).hasSize(0);

      match =
          comparisonExpression.match(
              "path", EnumSet.of(ValueComparisonOperator.EQ), JsonType.STRING, false);
      assertThat(match).hasSize(0);
    }

    @Test
    public void matchTestWithMapSetListComponent() throws Exception {
      final ComparisonExpression comparisonExpression =
          new ComparisonExpression(
              "mapSetListColumn",
              MapSetListComponent.LIST_VALUE,
              List.of(
                  ValueComparisonOperation.build(
                      ValueComparisonOperator.IN,
                      List.of("value1", "value2"),
                      MapSetListComponent.LIST_VALUE)),
              null);

      List<FilterOperation<?>> match =
          comparisonExpression.match(
              "*", EnumSet.of(ValueComparisonOperator.IN), JsonType.ARRAY, true);
      assertThat(match).hasSize(1);

      // MapSetListComponent should not exist in the ComparisonExpression and FilterOperation.
      match =
          comparisonExpression.match(
              "*", EnumSet.of(ValueComparisonOperator.IN), JsonType.NULL, false);
      assertThat(match).hasSize(0);

      // JsonType does not match
      match =
          comparisonExpression.match(
              "*", EnumSet.of(ValueComparisonOperator.IN), JsonType.NULL, true);
      assertThat(match).hasSize(0);

      // Operator does not match
      match =
          comparisonExpression.match(
              "*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.NULL, true);
      assertThat(match).hasSize(0);
    }
  }
}
