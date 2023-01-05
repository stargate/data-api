package io.stargate.sgv3.docsapi.api.model.command.filter;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.ValueComparisonOperation;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.ValueComparisonOperator;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
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
}
