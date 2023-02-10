package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ArrayComparisonOperator;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ElementComparisonOperator;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    public void mustHandleExists() throws Exception {
      String json =
          """
                        {"existsPath" : {"$exists": false}}
                        """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "existsPath",
              List.of(
                  new ValueComparisonOperation(
                      ElementComparisonOperator.EXISTS, new JsonLiteral(false, JsonType.BOOLEAN))));
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.comparisonExpressions()).hasSize(1).contains(expectedResult);
    }

    @Test
    public void mustHandleAll() throws Exception {
      String json =
          """
                            {"allPath" : {"$all": ["a", "b"]}}
                            """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "allPath",
              List.of(
                  new ValueComparisonOperation(
                      ArrayComparisonOperator.ALL,
                      new JsonLiteral(List.of("a", "b"), JsonType.ARRAY))));
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.comparisonExpressions()).hasSize(1).contains(expectedResult);
    }

    @Test
    public void mustHandleSize() throws Exception {
      String json =
          """
                            {"sizePath" : {"$size": 2}}
                            """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "sizePath",
              List.of(
                  new ValueComparisonOperation(
                      ArrayComparisonOperator.SIZE,
                      new JsonLiteral(new BigDecimal(2), JsonType.NUMBER))));
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.comparisonExpressions()).hasSize(1).contains(expectedResult);
    }

    @Test
    public void mustHandleSubDocEq() throws Exception {
      String json =
          """
                                {"sub_doc" : {"col": 2}}
                                """;
      Map<String, Object> value = new LinkedHashMap<>();
      value.put("col", new BigDecimal(2));
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "sub_doc",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral(value, JsonType.SUB_DOC))));
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.comparisonExpressions()).hasSize(1).contains(expectedResult);
    }
  }
}
