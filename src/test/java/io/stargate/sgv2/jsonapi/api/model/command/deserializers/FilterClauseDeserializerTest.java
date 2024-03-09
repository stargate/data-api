package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.*;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
                      ValueComparisonOperator.EQ, new JsonLiteral("aaron", JsonType.STRING))),
              null);
      assertThat(filterClause).isNotNull();
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
    }

    private static Stream<Arguments> provideRangeQueries() {
      return Stream.of(
          Arguments.of(
              "{\"username\": {\"$eq\" : \"aaron\"}}", ValueComparisonOperator.EQ, "username"),
          Arguments.of("{\"amount\": {\"$gt\" : 5000}}", ValueComparisonOperator.GT, "amount"),
          Arguments.of("{\"amount\": {\"$gte\" : 5000}}", ValueComparisonOperator.GTE, "amount"),
          Arguments.of("{\"amount\": {\"$lt\" : 5000}}", ValueComparisonOperator.LT, "amount"),
          Arguments.of("{\"amount\": {\"$lte\" : 5000}}", ValueComparisonOperator.LTE, "amount"),
          Arguments.of(
              "{\"dob\": {\"$gte\" : {\"$date\" : 1672531200000}}}",
              ValueComparisonOperator.GTE,
              "dob"));
    }

    @ParameterizedTest
    @MethodSource("provideRangeQueries")
    public void testRangeComparisonOperator(
        String json, ValueComparisonOperator operator, String column) throws Exception {
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause).isNotNull();
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause
                  .logicalExpression()
                  .comparisonExpressions
                  .get(0)
                  .getFilterOperations()
                  .get(0)
                  .operator())
          .isEqualTo(operator);
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(column);
    }

    @Test
    public void mustErrorNonNumberAndDateRange() throws Exception {
      String json = """
        {"amount": {"$gte" : "ABC"}}
        """;

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, FilterClause.class));
      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .isEqualTo(
                        "Invalid filter expression, $gte operator must have `DATE` or `NUMBER` value");
              });
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
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(0);
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
                      ValueComparisonOperator.EQ, new JsonLiteral("aaron", JsonType.STRING))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
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
                      new JsonLiteral(BigDecimal.valueOf(40), JsonType.NUMBER))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
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
                      ValueComparisonOperator.EQ, new JsonLiteral(true, JsonType.BOOLEAN))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
    }

    @Test
    public void mustHandleDate() throws Exception {
      String json = """
            {"dateType": {"$date": 1672531200000}}
          """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "dateType",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ,
                      new JsonLiteral(new Date(1672531200000L), JsonType.DATE))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
    }

    @Test
    public void mustHandleDateAndOr() throws Exception {
      String json =
          """
               { "$and" : [{"dateType": {"$date": 1672531200000}}]}
          """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "dateType",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ,
                      new JsonLiteral(new Date(1672531200000L), JsonType.DATE))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(1);
      assertThat(filterClause.logicalExpression().getTotalComparisonExpressionCount()).isEqualTo(1);
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(0)
                  .getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(0)
                  .getPath())
          .isEqualTo(expectedResult.getPath());
    }

    @Test
    public void mustHandleDateAsEpoch() throws Exception {
      String json = """
         {"dateType": {"$date": "2023-01-01"}}
        """;

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, FilterClause.class));
      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .isEqualTo(
                        "Invalid filter expression: $date value has to be sent as epoch time");
              });
    }

    @Test
    public void mustHandleDateAsEpochAndOr() throws Exception {
      String json = """
         { "$or" : [{"dateType": {"$date": "2023-01-01"}}]}
        """;

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, FilterClause.class));
      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .isEqualTo(
                        "Invalid filter expression: $date value has to be sent as epoch time");
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
                      new JsonLiteral(List.of("a", "b"), JsonType.ARRAY))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
    }

    @Test
    public void mustHandleAllAndOr() throws Exception {
      String json =
          """
                    {
                           "$or": [
                               {"allPath" : {"$all": ["a", "b"]}},
                               {
                                   "age": "testAge"
                               }
                           ]
                       }
                                """;
      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "allPath",
              List.of(
                  new ValueComparisonOperation(
                      ArrayComparisonOperator.ALL,
                      new JsonLiteral(List.of("a", "b"), JsonType.ARRAY))),
              null);
      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "age",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("testAge", JsonType.STRING))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(1);
      assertThat(filterClause.logicalExpression().getTotalComparisonExpressionCount()).isEqualTo(2);
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(0)
                  .getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(0)
                  .getPath())
          .isEqualTo(expectedResult1.getPath());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(1)
                  .getFilterOperations())
          .isEqualTo(expectedResult2.getFilterOperations());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(1)
                  .getPath())
          .isEqualTo(expectedResult2.getPath());
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
                      new JsonLiteral(new BigDecimal(2), JsonType.NUMBER))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
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
                assertThat(t.getMessage()).isEqualTo("$size operator must have integer value >= 0");
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
                      ValueComparisonOperator.EQ, new JsonLiteral(value, JsonType.SUB_DOC))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
    }

    @Test
    public void mustHandleArrayNe() throws Exception {
      String json = """
         {"col" : {"$ne": ["1","2"]}}
        """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "col",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE,
                      new JsonLiteral(List.of("1", "2"), JsonType.ARRAY))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
    }

    @Test
    public void mustHandleArrayEq() throws Exception {
      String json = """
         {"col" : {"$eq": ["3","4"]}}
        """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "col",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ,
                      new JsonLiteral(List.of("3", "4"), JsonType.ARRAY))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
    }

    @Test
    public void mustHandleSubDocNe() throws Exception {
      String json = """
         {"sub_doc" : {"$ne" : {"col": 2}}}
        """;
      Map<String, Object> value = new LinkedHashMap<>();
      value.put("col", new BigDecimal(2));
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "sub_doc",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE, new JsonLiteral(value, JsonType.SUB_DOC))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
    }

    @Test
    public void mustHandleMultiFilterWithOneField() throws Exception {
      String json =
          """
                {
                   "name": {
                       "$ne": "Tim",
                       "$exists": true
                   }
                }
              """;
      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "name",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE, new JsonLiteral("Tim", JsonType.STRING))),
              null);

      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "name",
              List.of(
                  new ValueComparisonOperation(
                      ElementComparisonOperator.EXISTS, new JsonLiteral(true, JsonType.BOOLEAN))),
              null);

      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(2);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult1.getPath());
      assertThat(
              filterClause
                  .logicalExpression()
                  .comparisonExpressions
                  .get(1)
                  .getFilterOperations()
                  .get(0))
          .isEqualTo(expectedResult2.getFilterOperations().get(0));
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(1).getPath())
          .isEqualTo(expectedResult2.getPath());
    }

    @Test
    public void mustHandleIdFieldIn() throws Exception {
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
                          JsonType.ARRAY))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
    }

    @Test
    public void mustHandleNonIdFieldIn() throws Exception {
      String json = """
               {"name" : {"$in": ["name1", "name2"]}}
              """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "name",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.IN,
                      new JsonLiteral(List.of("name1", "name2"), JsonType.ARRAY))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
    }

    @Test
    public void mustHandleNonIdFieldInAndOr() throws Exception {
      String json =
          """
               {
                           "$and": [
                               {"name" : {"$in": ["name1", "name2"]}},
                               {
                                   "age": "testAge"
                               }
                           ]
                       }
              """;
      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "name",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.IN,
                      new JsonLiteral(List.of("name1", "name2"), JsonType.ARRAY))),
              null);
      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "age",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("testAge", JsonType.STRING))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(1);
      assertThat(filterClause.logicalExpression().getTotalComparisonExpressionCount()).isEqualTo(2);
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(0)
                  .getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(0)
                  .getPath())
          .isEqualTo(expectedResult1.getPath());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(1)
                  .getFilterOperations())
          .isEqualTo(expectedResult2.getFilterOperations());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(1)
                  .getPath())
          .isEqualTo(expectedResult2.getPath());
    }

    @Test
    public void simpleOr() throws Exception {

      String json =
          """
              {
                                "$or":[
                                    {"name" : "testName"},
                                    {"age" : "testAge"}
                                 ]
                              }
              """;
      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "name",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("testName", JsonType.STRING))),
              null);
      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "age",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("testAge", JsonType.STRING))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(1);
      assertThat(filterClause.logicalExpression().logicalExpressions.get(0).comparisonExpressions)
          .hasSize(2);
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(0)
                  .getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(1)
                  .getFilterOperations())
          .isEqualTo(expectedResult2.getFilterOperations());
    }

    @Test
    public void simpleAnd() throws Exception {

      String json =
          """
              {
                                "$and":[
                                    {"name" : "testName"},
                                    {"age" : "testAge"}
                                 ]
                              }
              """;
      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "name",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("testName", JsonType.STRING))),
              null);
      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "age",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("testAge", JsonType.STRING))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(1);
      assertThat(filterClause.logicalExpression().logicalExpressions.get(0).comparisonExpressions)
          .hasSize(2);
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(0)
                  .getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(1)
                  .getFilterOperations())
          .isEqualTo(expectedResult2.getFilterOperations());
    }

    @Test
    public void simpleNot() throws Exception {
      String json =
          """
          {
            "$not": {"name" : "testName"}
          }
          """;
      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "name",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE, new JsonLiteral("testName", JsonType.STRING))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions.size()).isEqualTo(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
    }

    @Test
    public void comparisonOperatorsWithNot() throws Exception {
      String json =
          """
              {
                "$not": {
                  "$and" : [
                    {"f1" : {"$eq" : "testName"}},
                    {"f2" : {"$ne" : "testName"}},
                    {"f3" : {"$in" : ["testName1","testName2"]}},
                    {"f4" : {"$nin" : ["testName1","testName2"]}},
                    {"f5" : {"$lt" : 5}},
                    {"f6" : {"$lte" : 5}},
                    {"f7" : {"$gt" : 5}},
                    {"f8" : {"$gte" : 5}},
                    {"f9" : {"$exists" : true}},
                    {"f10" : {"$exists" : false}},
                    {"f11" : {"$size" : 1}}
                 ]
                }
              }
              """;
      final ComparisonExpression eq =
          new ComparisonExpression(
              "f1",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE, new JsonLiteral("testName", JsonType.STRING))),
              null);
      final ComparisonExpression ne =
          new ComparisonExpression(
              "f2",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("testName", JsonType.STRING))),
              null);
      final ComparisonExpression in =
          new ComparisonExpression(
              "f3",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NIN,
                      new JsonLiteral(List.of("testName1", "testName2"), JsonType.ARRAY))),
              null);

      final ComparisonExpression nin =
          new ComparisonExpression(
              "f4",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.IN,
                      new JsonLiteral(List.of("testName1", "testName2"), JsonType.ARRAY))),
              null);
      final ComparisonExpression lt =
          new ComparisonExpression(
              "f5",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.GTE,
                      new JsonLiteral(new BigDecimal(5), JsonType.NUMBER))),
              null);

      final ComparisonExpression lte =
          new ComparisonExpression(
              "f6",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.GT,
                      new JsonLiteral(new BigDecimal(5), JsonType.NUMBER))),
              null);

      final ComparisonExpression gt =
          new ComparisonExpression(
              "f7",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.LTE,
                      new JsonLiteral(new BigDecimal(5), JsonType.NUMBER))),
              null);
      final ComparisonExpression gte =
          new ComparisonExpression(
              "f8",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.LT,
                      new JsonLiteral(new BigDecimal(5), JsonType.NUMBER))),
              null);

      final ComparisonExpression existsTrue =
          new ComparisonExpression(
              "f9",
              List.of(
                  new ValueComparisonOperation(
                      ElementComparisonOperator.EXISTS, new JsonLiteral(false, JsonType.BOOLEAN))),
              null);
      final ComparisonExpression existsFalse =
          new ComparisonExpression(
              "f10",
              List.of(
                  new ValueComparisonOperation(
                      ElementComparisonOperator.EXISTS, new JsonLiteral(true, JsonType.BOOLEAN))),
              null);
      final ComparisonExpression size =
          new ComparisonExpression(
              "f11",
              List.of(
                  new ValueComparisonOperation(
                      ArrayComparisonOperator.SIZE,
                      new JsonLiteral(new BigDecimal(-1), JsonType.NUMBER))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(1);
      assertThat(filterClause.logicalExpression().logicalExpressions.get(0).getLogicalRelation())
          .isEqualTo(LogicalExpression.LogicalOperator.OR);
      assertThat(filterClause.logicalExpression().logicalExpressions.get(0).comparisonExpressions)
          .hasSize(11);
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(0)
                  .getFilterOperations())
          .isEqualTo(eq.getFilterOperations());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(1)
                  .getFilterOperations())
          .isEqualTo(ne.getFilterOperations());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(2)
                  .getFilterOperations())
          .isEqualTo(in.getFilterOperations());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(3)
                  .getFilterOperations())
          .isEqualTo(nin.getFilterOperations());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(4)
                  .getFilterOperations())
          .isEqualTo(lt.getFilterOperations());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(5)
                  .getFilterOperations())
          .isEqualTo(lte.getFilterOperations());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(6)
                  .getFilterOperations())
          .isEqualTo(gt.getFilterOperations());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(7)
                  .getFilterOperations())
          .isEqualTo(gte.getFilterOperations());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(8)
                  .getFilterOperations())
          .isEqualTo(existsTrue.getFilterOperations());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(9)
                  .getFilterOperations())
          .isEqualTo(existsFalse.getFilterOperations());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(10)
                  .getFilterOperations())
          .isEqualTo(size.getFilterOperations());
    }

    @Test
    public void twoLevelNot() throws Exception {
      String json =
          """
              {
                "$not":{ "$not" : {"name" : "testName"} }
              }
              """;
      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "name",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("testName", JsonType.STRING))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions.size()).isEqualTo(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
    }

    @Test
    public void multipleNot() throws Exception {
      String json =
          """
            {
              "$not": { "$not" : { "$not" : {"name" : "testName"} } }
            }
            """;
      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "name",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE, new JsonLiteral("testName", JsonType.STRING))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions.size()).isEqualTo(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
    }

    @Test
    public void nestedOrAnd() throws Exception {

      String json =
          """
        {
          "$and": [
            {
                "name": "testName"
            },
            {
                "age": "testAge"
            },
            {
                "$or": [
                    {
                        "address": "testAddress"
                    },
                    {
                        "height": "testHeight"
                    }
                ]
            }
          ]
        }
        """;
      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "name",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("testName", JsonType.STRING))),
              null);
      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "age",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("testAge", JsonType.STRING))),
              null);
      final ComparisonExpression expectedResult3 =
          new ComparisonExpression(
              "address",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("testAddress", JsonType.STRING))),
              null);
      final ComparisonExpression expectedResult4 =
          new ComparisonExpression(
              "height",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("testHeight", JsonType.STRING))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(1);
      assertThat(filterClause.logicalExpression().getTotalComparisonExpressionCount()).isEqualTo(4);
      assertThat(filterClause.logicalExpression().logicalExpressions.get(0).comparisonExpressions)
          .hasSize(2);
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions)
          .hasSize(2);

      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(0)
                  .getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(1)
                  .getFilterOperations())
          .isEqualTo(expectedResult2.getFilterOperations());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(0)
                  .getFilterOperations())
          .isEqualTo(expectedResult3.getFilterOperations());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(1)
                  .getFilterOperations())
          .isEqualTo(expectedResult4.getFilterOperations());
    }

    @Test
    public void multipleLevel() throws Exception {
      String json =
          """
        {
          "$and": [
            {
                "name": "testName"
            },
            {
                "age": "testAge"
            },
            {
              "$not":
              {
                "$or": [
                    {
                        "address": "testAddress"
                    },
                    {
                        "tags": { "$size" : 1}
                    }
                ]
             }
            }
          ]
        }
        """;
      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "name",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("testName", JsonType.STRING))),
              null);
      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "age",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("testAge", JsonType.STRING))),
              null);
      final ComparisonExpression expectedResult3 =
          new ComparisonExpression(
              "address",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE, new JsonLiteral("testAddress", JsonType.STRING))),
              null);
      final ComparisonExpression expectedResult4 =
          new ComparisonExpression(
              "tags",
              List.of(
                  new ValueComparisonOperation(
                      ArrayComparisonOperator.SIZE,
                      new JsonLiteral(new BigDecimal(-1), JsonType.NUMBER))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions.get(0).getLogicalRelation())
          .isEqualTo(LogicalExpression.LogicalOperator.AND);
      assertThat(filterClause.logicalExpression().logicalExpressions.get(0).comparisonExpressions)
          .hasSize(2);
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(0)
                  .getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(1)
                  .getFilterOperations())
          .isEqualTo(expectedResult2.getFilterOperations());

      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .logicalExpressions
                  .get(0)
                  .getLogicalRelation())
          .isEqualTo(LogicalExpression.LogicalOperator.AND);
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions)
          .hasSize(2);
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(0)
                  .getFilterOperations())
          .isEqualTo(expectedResult3.getFilterOperations());
      assertThat(
              filterClause
                  .logicalExpression()
                  .logicalExpressions
                  .get(0)
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(1)
                  .getFilterOperations())
          .isEqualTo(expectedResult4.getFilterOperations());
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
                      ValueComparisonOperator.IN, new JsonLiteral(List.of(), JsonType.ARRAY))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
    }

    @Test
    public void mustHandleNinArrayNonEmpty() throws Exception {
      String json = """
               {"_id" : {"$nin": []}}
              """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "_id",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NIN, new JsonLiteral(List.of(), JsonType.ARRAY))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
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
    public void mustHandleNinArrayOnly() throws Exception {
      String json = """
               {"_id" : {"$nin": "random"}}
              """;
      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, FilterClause.class));
      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage()).isEqualTo("$nin operator must have `ARRAY`");
              });
    }

    @Test
    public void mustHandleNinArrayOnlyAnd() throws Exception {
      String json =
          """
               {
                           "$and": [
                               {"age" : {"$nin": "aaa"}},
                               {
                                   "name": "testName"
                               }
                           ]
                       }
              """;
      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, FilterClause.class));
      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage()).isEqualTo("$nin operator must have `ARRAY`");
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

    @Test
    public void mustHandleNinArrayWithBigArray() throws Exception {
      // String array with 100 unique numbers
      String json =
          """
            {"_id" : {"$nin": ["0","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70","71","72","73","74","75","76","77","78","79","80","81","82","83","84","85","86","87","88","89","90","91","92","93","94","95","96","97","98","99","100"]}}
           """;
      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, FilterClause.class));
      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .isEqualTo(
                        "$nin operator must have at most "
                            + operationsConfig.maxInOperatorValueSize()
                            + " values");
              });
    }

    @Test
    public void multipleIdFilterAndOr() throws Exception {
      String json =
          """
               {
                                  "_id": "testID1",
                                  "$or": [
                                      {
                                          "name": "testName"
                                      },
                                      {
                                          "age": "testAge"
                                      },
                                      {
                                          "_id": "testID2"
                                      }
                                  ]
                              }
              """;
      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, FilterClause.class));
      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .isEqualTo(ErrorCode.FILTER_MULTIPLE_ID_FILTER.getMessage());
              });
    }

    @Test
    public void invalidPathName() throws Exception {
      String json = """
              {"$gt" : {"test" : 5}}
          """;
      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, FilterClause.class));

      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .isEqualTo(
                        "Invalid filter expression: filter clause path ('$gt') contains character(s) not allowed");
              });
    }

    @Test
    public void valid$vectorPathName() throws Exception {
      String json = """
              {"$vector" : {"$exists": true}}
              """;

      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo("$vector");
      assertThat(
              filterClause
                  .logicalExpression()
                  .comparisonExpressions
                  .get(0)
                  .getFilterOperations()
                  .get(0)
                  .operator())
          .isEqualTo(ElementComparisonOperator.EXISTS);
    }

    @Test
    public void invalid$vectorPathName() throws Exception {
      String json = """
              {"$exists" : {"$vector": true}}
              """;

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, FilterClause.class));

      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage()).isEqualTo("Unsupported filter operator: $vector");
              });
    }

    @Test
    public void invalidPathNameWithValidOperator() {
      String json = """
              {"$exists" : {"$exists": true}}
              """;
      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, FilterClause.class));

      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .isEqualTo(
                        "Invalid filter expression: filter clause path ('$exists') contains character(s) not allowed");
              });
    }
  }

  @Nested
  class DeserializeWithJsonExtensions {
    @Test
    public void mustHandleUUIDAsId() throws Exception {
      final String UUID = "16725312-0000-0000-0000-000000000000";
      String json = """
            {"_id": {"$uuid": "%s"}}
          """.formatted(UUID);
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "_id",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral(UUID, JsonType.DOCUMENT_ID))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
    }

    @Test
    public void mustHandleUUIDAsRegularField() throws Exception {
      final String UUID = "16725312-0000-0000-0000-000000000000";
      String json = """
            {"value": {"$uuid": "%s"}}
          """.formatted(UUID);
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "value",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral(UUID, JsonType.STRING))),
              null);
      FilterClause filterClause = objectMapper.readValue(json, FilterClause.class);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
    }

    @Test
    public void mustFailOnBadUUIDAsId() throws Exception {
      String json = """
         {"_id": {"$uuid": "abc"}}
        """;

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, FilterClause.class));
      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .isEqualTo(
                        "Bad JSON Extension value: '$uuid' value has invalid contents (\"abc\")");
              });
    }

    @Test
    public void mustFailOnUnknownOperatorAsId() throws Exception {
      String json = """
         {"_id": {"$GUID": "abc"}}
        """;

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, FilterClause.class));
      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage()).isEqualTo("Unsupported filter operator: $GUID");
              });
    }

    @Test
    public void mustFailOnBadUUIDAsField() throws Exception {
      String json = """
         {"field": {"$uuid": "abc"}}
        """;

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, FilterClause.class));
      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .isEqualTo(
                        "Bad JSON Extension value: '$uuid' value has invalid contents (\"abc\")");
              });
    }
  }
}
