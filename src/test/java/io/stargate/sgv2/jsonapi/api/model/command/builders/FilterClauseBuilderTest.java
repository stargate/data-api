package io.stargate.sgv2.jsonapi.api.model.command.builders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.*;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.collections.JsonExtensionType;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.io.IOException;
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
public class FilterClauseBuilderTest {

  @Inject ObjectMapper objectMapper;
  @Inject OperationsConfig operationsConfig;

  private final TestConstants testConstants = new TestConstants();

  @Nested
  class BuildWithRegularOperators {

    @Test
    public void happyPath() throws Exception {
      String json =
          """
                    {"username": "aaron"}
                    """;

      FilterClause filterClause = readCollectionFilterClause(json);
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "username",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, "aaron")),
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
              "dob"),
          Arguments.of(
              "{\"stringColumn\": {\"$lte\" : \"abc\"}}",
              ValueComparisonOperator.LTE,
              "stringColumn"),
          Arguments.of(
              "{\"boolColumn\": {\"$lte\" : false}}", ValueComparisonOperator.LTE, "boolColumn"));
    }

    @ParameterizedTest
    @MethodSource("provideRangeQueries")
    public void testRangeComparisonOperator(
        String json, ValueComparisonOperator operator, String column) throws Exception {
      FilterClause filterClause = readCollectionFilterClause(json);
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
    public void mustHandleNull() throws Exception {
      String json = "null";

      FilterClause filterClause = readCollectionFilterClause(json);

      assertThat(filterClause.isEmpty()).isTrue();
    }

    @Test
    public void mustHandleEmpty() throws Exception {
      String json =
          """
                    {}
                    """;

      FilterClause filterClause = readCollectionFilterClause(json);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(0);
    }

    @Test
    public void mustHandleString() throws Exception {
      String json =
          """
                    {"username": "aaron"}
                    """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "username",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, "aaron")),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
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
      String json =
          """
                    {"numberType": 40}
                    """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "numberType",
              List.of(
                  ValueComparisonOperation.build(
                      ValueComparisonOperator.EQ, BigDecimal.valueOf(40))),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
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
      String json =
          """
                    {"boolType": true}
                    """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "boolType",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, true)),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
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
      String json =
          """
            {"dateType": {"$date": 1672531200000}}
          """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "dateType",
              List.of(
                  ValueComparisonOperation.build(
                      ValueComparisonOperator.EQ, new Date(1672531200000L))),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
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
                  ValueComparisonOperation.build(
                      ValueComparisonOperator.EQ, new Date(1672531200000L))),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
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
      String json =
          """
         {"dateType": {"$date": "2023-01-01"}}
        """;

      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));
      assertThat(throwable)
          .isInstanceOf(DocumentException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .startsWith(
                        "Bad JSON Extension value to shred: '$date' value has to be an epoch timestamp, instead got (\"2023-01-01\")");
              });
    }

    @Test
    public void mustHandleDateAsEpochAndOr() throws Exception {
      String json =
          """
         { "$or" : [{"dateType": {"$date": "2023-01-01"}}]}
        """;

      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));
      assertThat(throwable)
          .isInstanceOf(DocumentException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .startsWith(
                        "Bad JSON Extension value to shred: '$date' value has to be an epoch timestamp, instead got (\"2023-01-01\")");
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
                  ValueComparisonOperation.build(ArrayComparisonOperator.ALL, List.of("a", "b"))),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
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
                  ValueComparisonOperation.build(ArrayComparisonOperator.ALL, List.of("a", "b"))),
              null);
      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "age",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, "testAge")),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
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
      String json =
          """
          {"allPath" : {"$all": "abc"}}
        """;
      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));
      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage()).contains("'$all' operator must have `Array` value");
              });
    }

    @Test
    public void mustHandleAllNonEmptyArray() throws Exception {
      String json =
          """
          {"allPath" : {"$all": []}}
        """;
      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));
      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage()).contains("'$all' operator must have at least one value");
              });
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
                  ValueComparisonOperation.build(ArrayComparisonOperator.SIZE, new BigDecimal(2))),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
    }

    @Test
    public void mustHandleIntegerWithTrailingZeroSize() throws Exception {
      String json =
          """
                        {"sizePath" : {"$size": 0.0}}
                      """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "sizePath",
              List.of(
                  ValueComparisonOperation.build(ArrayComparisonOperator.SIZE, new BigDecimal(0))),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());

      String json1 =
          """
                        {"sizePath" : {"$size": 5.0}}
                      """;
      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "sizePath",
              List.of(
                  ValueComparisonOperation.build(ArrayComparisonOperator.SIZE, new BigDecimal(5))),
              null);
      FilterClause filterClause1 = readCollectionFilterClause(json);
      assertThat(filterClause1.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause1.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause1.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(filterClause1.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
    }

    @Test
    public void mustHandleSizeNonNumber() throws Exception {
      String json =
          """
            {"sizePath" : {"$size": "2"}}
          """;
      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));
      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage()).contains("'$size' operator must have integer");
              });
    }

    // Notice, 0.0, -0.0, 5.0, etc are still considered as Integer
    @Test
    public void mustHandleSizeNonInteger() throws Exception {
      String json =
          """
                        {"sizePath" : {"$size": "1.1"}}
                      """;
      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));
      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage()).contains("'$size' operator must have integer");
              });

      String json1 =
          """
                        {"sizePath" : {"$size": "5.4"}}
                      """;
      Throwable throwable1 = catchThrowable(() -> readCollectionFilterClause(json1));
      assertThat(throwable1)
          .isInstanceOf(FilterException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage()).contains("'$size' operator must have integer");
              });
    }

    @Test
    public void mustHandleSizeNegative() throws Exception {
      String json =
          """
                        {"sizePath" : {"$size": -2}}
                      """;
      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));
      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .contains("'$size' operator must have integer value >= 0 (had -2)");
              });
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
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, value)),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
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
      String json =
          """
                       {"col" : {"$ne": ["1","2"]}}
                      """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "col",
              List.of(
                  ValueComparisonOperation.build(ValueComparisonOperator.NE, List.of("1", "2"))),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
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
      String json =
          """
                       {"col" : {"$eq": ["3","4"]}}
                      """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "col",
              List.of(
                  ValueComparisonOperation.build(ValueComparisonOperator.EQ, List.of("3", "4"))),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
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
      String json =
          """
                       {"sub_doc" : {"$ne" : {"col": 2}}}
                      """;
      Map<String, Object> value = new LinkedHashMap<>();
      value.put("col", new BigDecimal(2));
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "sub_doc",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.NE, value)),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
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
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.NE, "Tim")),
              null);

      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "name",
              List.of(ValueComparisonOperation.build(ElementComparisonOperator.EXISTS, true)),
              null);

      FilterClause filterClause = readCollectionFilterClause(json);
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
      String json =
          """
           {"_id" : {"$in": ["2", "3"]}}
          """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "_id",
              List.of(
                  ValueComparisonOperation.build(
                      ValueComparisonOperator.IN,
                      List.of(DocumentId.fromString("2"), DocumentId.fromString("3")))),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
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
      String json =
          """
           {"name" : {"$in": ["name1", "name2"]}}
          """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "name",
              List.of(
                  ValueComparisonOperation.build(
                      ValueComparisonOperator.IN, List.of("name1", "name2"))),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
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
                  ValueComparisonOperation.build(
                      ValueComparisonOperator.IN, List.of("name1", "name2"))),
              null);
      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "age",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, "testAge")),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
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
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, "testName")),
              null);
      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "age",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, "testAge")),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
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
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, "testName")),
              null);
      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "age",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, "testAge")),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
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
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, "testName")),
              null);
      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "age",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, "testAge")),
              null);
      final ComparisonExpression expectedResult3 =
          new ComparisonExpression(
              "address",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, "testAddress")),
              null);
      final ComparisonExpression expectedResult4 =
          new ComparisonExpression(
              "height",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, "testHeight")),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
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
    public void mustHandleInArrayNonEmpty() throws Exception {
      String json =
          """
                       {"_id" : {"$in": []}}
                      """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "_id",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.IN, List.of())),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
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
      String json =
          """
                       {"_id" : {"$nin": []}}
                      """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "_id",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.NIN, List.of())),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
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
      String json =
          """
                       {"_id" : {"$in": "aaa"}}
                      """;
      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));
      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage()).contains("'$in' operator must have `Array`");
              });
    }

    @Test
    public void mustHandleNinArrayOnly() throws Exception {
      String json =
          """
                       {"_id" : {"$nin": "random"}}
                      """;
      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));
      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage()).contains("'$nin' operator must have `Array`");
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
      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));
      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage()).contains("'$nin' operator must have `Array`");
              });
    }

    @Test
    public void mustHandleInArrayWithBigArray() throws Exception {
      // String array with 100 unique numbers
      String json =
          """
                       {"_id" : {"$in": ["0","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70","71","72","73","74","75","76","77","78","79","80","81","82","83","84","85","86","87","88","89","90","91","92","93","94","95","96","97","98","99","100"]}}
                      """;
      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));
      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .contains(
                        "'$in' operator must have at most "
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
      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));
      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .contains(
                        "'$nin' operator must have at most "
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
      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));
      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .hasFieldOrPropertyWithValue(
              "code", FilterException.Code.FILTER_MULTIPLE_ID_FILTER.name());
    }

    @Test
    public void invalidPathName() throws Exception {
      String json =
          """
              {"$gt" : {"test" : 5}}
          """;
      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));

      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .startsWith(
                        "Unsupported filter clause: filter expression path ('$gt') cannot start with '$'");
              });
    }

    @Test
    public void valid$vectorPathName() throws Exception {
      String json =
          """
          {"$vector" : {"$exists": true}}
          """;

      FilterClause filterClause = readCollectionFilterClause(json);
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
      String json =
          """
              {"$exists" : {"$vector": true}}
              """;

      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));

      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage()).startsWith("Unsupported filter operator '$vector'");
              });
    }

    @Test
    public void invalidPathNameWithValidOperator() {
      String json =
          """
              {"$exists" : {"$exists": true}}
              """;
      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));

      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .startsWith(
                        "Unsupported filter clause: filter expression path ('$exists') cannot start with '$'");
              });
    }
  }

  @Nested
  class BuildWithMatchOperator {
    @Test
    public void mustHandleMatchOperator() throws Exception {
      String json =
          """
          {"$lexical": {"$match": "search text"}}
          """;
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "$lexical",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.MATCH,
                      new JsonLiteral("search text", JsonType.STRING),
                      null)),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
    }

    @Test
    public void mustFailOnMatchWithNonLexicalField() {
      String json =
          """
          {"content": {"$match": "search text"}}
          """;
      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));
      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .contains(
                        "'$match' operator can only be used with the '$lexical' field, not 'content'");
              });
    }

    @ParameterizedTest
    @MethodSource("matchNonStringArgs")
    public void mustFailOnMatchWithNonString(String actualType, String jsonSnippet) {
      String json =
              """
          {"$lexical": {"$match": %s}}
          """
              .formatted(jsonSnippet);
      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));
      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .contains(
                        "'$match' operator must have `String` value, was `%s`"
                            .formatted(actualType));
              });
    }

    private static Stream<Arguments> matchNonStringArgs() {
      return Stream.of(
          Arguments.of("Array", "[\"text1\", \"text2\"]"),
          Arguments.of("Boolean", "true"),
          Arguments.of("Null", "null"),
          Arguments.of("Number", "42"),
          Arguments.of("Object", "{\"key\": \"value\"}"));
    }

    // Verify explicit "$eq" not allowed for $lexical
    @Test
    public void mustFailOnLexicalWithExplicitEq() {
      String json =
          """
              {"$lexical": { "$eq": "search text"} }
              """;
      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));
      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .contains(
                        "cannot filter on '$lexical' field using operator '$eq': only '$match' is supported");
              });
    }

    // Verify short-cut for "$eq" not allowed for $lexical
    @Test
    public void mustFailOnLexicalWithImplicitEq() {
      String json =
          """
                  {"$lexical": "search text"}
                  """;
      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));
      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .contains(
                        "cannot filter on '$lexical' field using operator '$eq': only '$match' is supported");
              });
    }
  }

  @Nested
  class BuildWithJsonExtensions {
    @Test
    public void mustHandleObjectIdAsId() throws Exception {
      final String OBJECT_ID = "5f3e3d1e1e6e6f1e6e6e6f1e";
      String json =
              """
            {"_id": {"$objectId": "%s"}}
          """
              .formatted(OBJECT_ID);
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "_id",
              List.of(
                  ValueComparisonOperation.build(
                      ValueComparisonOperator.EQ,
                      DocumentId.fromExtensionType(
                          JsonExtensionType.OBJECT_ID,
                          objectMapper.getNodeFactory().textNode(OBJECT_ID)))),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
    }

    @Test
    public void mustHandleObjectIdAsRegularField() throws Exception {
      final String OBJECT_ID = "5f3e3d1e1e6e6f1e6e6e6f1e";
      String json =
              """
            {"nested.path": {"$objectId": "%s"}}
          """
              .formatted(OBJECT_ID);
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "nested.path",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, OBJECT_ID)),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
    }

    @Test
    public void mustHandleUUIDAsId() throws Exception {
      final String UUID = "16725312-0000-0000-0000-000000000000";
      String json =
              """
            {"_id": {"$uuid": "%s"}}
          """
              .formatted(UUID);
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "_id",
              List.of(
                  ValueComparisonOperation.build(
                      ValueComparisonOperator.EQ,
                      DocumentId.fromExtensionType(
                          JsonExtensionType.UUID, objectMapper.getNodeFactory().textNode(UUID)))),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(1);
      assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
          .isEqualTo(expectedResult.getPath());
      assertThat(
              filterClause.logicalExpression().comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult.getFilterOperations());
    }

    @Test
    public void mustHandleUUIDAsRegularField() throws Exception {
      final String UUID = "16725312-0000-0000-0000-000000000000";
      String json =
              """
            {"value": {"$uuid": "%s"}}
          """
              .formatted(UUID);
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "value",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, UUID)),
              null);
      FilterClause filterClause = readCollectionFilterClause(json);
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
      String json =
          """
         {"_id": {"$uuid": "abc"}}
        """;

      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));
      assertThat(throwable)
          .isInstanceOf(DocumentException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .contains(
                        "Bad JSON Extension value to shred: '$uuid' value has to be 36-character UUID String, instead got (\"abc\")");
              });
    }

    @Test
    public void mustFailOnBadObjectIdAsId() throws Exception {
      String json =
          """
         {"_id": {"$objectId": "xyz"}}
        """;

      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));
      assertThat(throwable)
          .isInstanceOf(DocumentException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .contains(
                        "Bad JSON Extension value to shred: '$objectId' value has to be 24-digit hexadecimal ObjectId, instead got (\"xyz\")");
              });
    }

    @Test
    public void mustFailOnUnknownOperatorAsId() throws Exception {
      String json =
          """
         {"_id": {"$GUID": "abc"}}
        """;

      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));
      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage()).startsWith("Unsupported filter operator '$GUID'");
              });
    }

    @Test
    public void mustFailOnBadUUIDAsField() throws Exception {
      String json =
          """
         {"field": {"$uuid": "abc"}}
        """;

      Throwable throwable = catchThrowable(() -> readCollectionFilterClause(json));
      assertThat(throwable)
          .isInstanceOf(DocumentException.class)
          .satisfies(
              t -> {
                assertThat(t.getMessage())
                    .startsWith(
                        "Bad JSON Extension value to shred: '$uuid' value has to be 36-character UUID String, instead got (\"abc\")");
              });
    }
  }

  FilterClause readCollectionFilterClause(String json) {
    try {
      return FilterClauseBuilder.builderFor(testConstants.COLLECTION_SCHEMA_OBJECT)
          .build(operationsConfig, objectMapper.readTree(json));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
