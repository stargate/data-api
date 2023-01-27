package io.stargate.sgv3.docsapi.api.model.command.deserializers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.ValueComparisonOperation;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv3.docsapi.exception.DocsException;
import java.math.BigDecimal;
import java.util.List;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FilterClauseDeserializerTest {

  @Inject ObjectMapper objectMapper;

  @Nested
  class Deserialize {

    @Test
    public void happyPath() throws Exception {
      String json = """
                    {"username": "aaron"}
                    """;

      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "username",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("aaron", JsonType.STRING))));
      assertThat(filterClause).isNotNull();
      assertThat(filterClause.comparisonExpressions()).hasSize(1).contains(expectedResult);
    }

    @Test
    public void eqComparisonOperator() throws Exception {
      String json = """
                    {"username": {"$eq" : "aaron"}}
                    """;

      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "username",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("aaron", JsonType.STRING))));
      assertThat(filterClause).isNotNull();
      assertThat(filterClause.comparisonExpressions()).hasSize(1).contains(expectedResult);
    }

    @Test
    public void gtComparisonOperator() throws Exception {
      String json = """
                    {"username": {"$gt" : 10}}
                    """;

      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "username",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.GT,
                      new JsonLiteral(new BigDecimal(10), JsonType.NUMBER))));
      assertThat(filterClause).isNotNull();
      assertThat(filterClause.comparisonExpressions()).hasSize(1).contains(expectedResult);
    }

    @Test
    public void neComparisonOperator() throws Exception {
      String json = """
                    {"username": {"$ne" : 10}}
                    """;

      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "username",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE,
                      new JsonLiteral(new BigDecimal(10), JsonType.NUMBER))));
      assertThat(filterClause).isNotNull();
      assertThat(filterClause.comparisonExpressions()).hasSize(1).contains(expectedResult);
    }

    @Test
    public void mustHandleNull() throws Exception {
      String json = "null";

      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);

      assertThat(filterClause).isNull();
    }

    @Test
    public void mustHandleEmpty() throws Exception {
      String json = """
                    {}
                    """;

      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);

      assertThat(filterClause.comparisonExpressions()).hasSize(0);
    }

    @Test
    public void mustHandleString() throws Exception {
      String json = """
                    {"username": "aaron"}
                    """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "username",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("aaron", JsonType.STRING))));
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.comparisonExpressions()).hasSize(1).contains(expectedResult);
    }

    @Test
    public void mustHandleNumber() throws Exception {
      String json = """
                    {"numberType": 40}
                    """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "numberType",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ,
                      new JsonLiteral(BigDecimal.valueOf(40), JsonType.NUMBER))));
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.comparisonExpressions()).hasSize(1).contains(expectedResult);
    }

    @Test
    public void mustHandleBoolean() throws Exception {
      String json = """
                    {"boolType": true}
                    """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "boolType",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral(true, JsonType.BOOLEAN))));
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.comparisonExpressions()).hasSize(1).contains(expectedResult);
    }

    @Test
    public void unsupportedFilterTypes() {
      String json = """
                    {"boolType": ["a"]}
                    """;

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, FilterClause.class));
      assertThat(throwable).isInstanceOf(DocsException.class);
    }
  }
}
