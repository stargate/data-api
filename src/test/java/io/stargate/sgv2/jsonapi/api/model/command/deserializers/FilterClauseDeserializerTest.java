package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ArrayComparisonOperator;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
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
  @Inject OperationsConfig operationsConfig;

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

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, FilterClause.class));
      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage()).isEqualTo("$exists operator supports only true");
              });
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
    public void mustHandleAllNonArray() throws Exception {
      String json = """
          {"allPath" : {"$all": "abc"}}
        """;
      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, FilterClause.class));
      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage()).isEqualTo("$all operator must have `ARRAY` value");
              });
    }

    @Test
    public void mustHandleAllNonEmptyArray() throws Exception {
      String json = """
          {"allPath" : {"$all": []}}
        """;
      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, FilterClause.class));
      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage()).isEqualTo("$all operator must have at least one value");
              });
    }

    @Test
    public void mustHandleSize() throws Exception {
      String json = """
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
    public void mustHandleSizeNonNumber() throws Exception {
      String json = """
          {"sizePath" : {"$size": "2"}}
        """;
      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, FilterClause.class));
      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage()).isEqualTo("$size operator must have integer");
              });
    }

    @Test
    public void mustHandleSizeNegative() throws Exception {
      String json = """
          {"sizePath" : {"$size": -2}}
        """;
      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, FilterClause.class));
      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .isEqualTo("$size operator must have interger value >= 0");
              });
    }

    @Test
    public void mustHandleSubDocEq() throws Exception {
      String json = """
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

    @Test
    public void mustHandleIn() throws Exception {
      String json = """
               {"_id" : {"$in": ["2", "3"]}}
              """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "_id",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.IN,
                      new JsonLiteral(
                          List.of(DocumentId.fromString("2"), DocumentId.fromString("3")),
                          JsonType.ARRAY))));
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.comparisonExpressions()).hasSize(1).contains(expectedResult);
    }

    @Test
    public void mustHandleInArrayNonEmpty() throws Exception {
      String json = """
               {"_id" : {"$in": []}}
              """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "_id",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.IN, new JsonLiteral(List.of(), JsonType.ARRAY))));
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.comparisonExpressions()).hasSize(1).contains(expectedResult);
    }

    @Test
    public void mustHandleInIdFieldOnly() throws Exception {
      String json = """
               {"name" : {"$in": ["aaa"]}}
              """;
      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, FilterClause.class));
      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage()).isEqualTo("Can use $in operator only on _id field");
              });
    }

    @Test
    public void mustHandleInArrayOnly() throws Exception {
      String json = """
               {"_id" : {"$in": "aaa"}}
              """;
      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, FilterClause.class));
      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage()).isEqualTo("$in operator must have `ARRAY`");
              });
    }

    @Test
    public void mustHandleInArrayWithBigArray() throws Exception {
      // String array with 100 unique numbers
      String json =
          """
        {"_id" : {"$in": ["0","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70","71","72","73","74","75","76","77","78","79","80","81","82","83","84","85","86","87","88","89","90","91","92","93","94","95","96","97","98","99","100"]}}
       """;
      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, FilterClause.class));
      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .isEqualTo(
                        "$in operator must have at most "
                            + operationsConfig.maxInOperatorValueSize()
                            + " values");
              });
    }
  }
}
