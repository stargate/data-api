package io.stargate.sgv2.jsonapi.service.resolver.matcher;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.*;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.MapComponentDesc;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FilterInversionTest {

  @Inject ObjectMapper objectMapper;
  @Inject OperationsConfig operationsConfig;

  private TestConstants testConstants = new TestConstants();

  CommandContext<CollectionSchemaObject> commandContext;

  @BeforeEach
  public void beforeEach() {
    commandContext = testConstants.collectionContext();
  }

  @Nested
  public class CollectionTest {

    @Test
    public void simpleNot() throws Exception {

      String json =
          """
                                {
                                   "find": {
                                       "filter": {
                                           "$not": {
                                               "name": "testName"
                                           }
                                       }
                                   }
                               }
                      """;
      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      final FilterClause filterClause = invertedFilterClause(commandContext, findCommand);
      final LogicalExpression logicalExpression = filterClause.logicalExpression();
      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "name",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.NE, "testName")),
              null);
      assertThat(logicalExpression.logicalExpressions).hasSize(0);
      assertThat(logicalExpression.comparisonExpressions.size()).isEqualTo(1);
      assertThat(logicalExpression.comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
    }

    @Test
    public void comparisonOperatorsWithNot() throws Exception {

      String json =
          """
                                {
                                   "find": {
                                       "filter": {
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
                                   }
                               }
                      """;
      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      final FilterClause filterClause = invertedFilterClause(commandContext, findCommand);
      final LogicalExpression logicalExpression = filterClause.logicalExpression();
      final ComparisonExpression eq =
          new ComparisonExpression(
              "f1",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.NE, "testName")),
              null);
      final ComparisonExpression ne =
          new ComparisonExpression(
              "f2",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, "testName")),
              null);
      final ComparisonExpression in =
          new ComparisonExpression(
              "f3",
              List.of(
                  ValueComparisonOperation.build(
                      ValueComparisonOperator.NIN, List.of("testName1", "testName2"))),
              null);

      final ComparisonExpression nin =
          new ComparisonExpression(
              "f4",
              List.of(
                  ValueComparisonOperation.build(
                      ValueComparisonOperator.IN, List.of("testName1", "testName2"))),
              null);
      final ComparisonExpression lt =
          new ComparisonExpression(
              "f5",
              List.of(
                  ValueComparisonOperation.build(ValueComparisonOperator.GTE, new BigDecimal(5))),
              null);

      final ComparisonExpression lte =
          new ComparisonExpression(
              "f6",
              List.of(
                  ValueComparisonOperation.build(ValueComparisonOperator.GT, new BigDecimal(5))),
              null);

      final ComparisonExpression gt =
          new ComparisonExpression(
              "f7",
              List.of(
                  ValueComparisonOperation.build(ValueComparisonOperator.LTE, new BigDecimal(5))),
              null);
      final ComparisonExpression gte =
          new ComparisonExpression(
              "f8",
              List.of(
                  ValueComparisonOperation.build(ValueComparisonOperator.LT, new BigDecimal(5))),
              null);

      final ComparisonExpression existsTrue =
          new ComparisonExpression(
              "f9",
              List.of(ValueComparisonOperation.build(ElementComparisonOperator.EXISTS, false)),
              null);
      final ComparisonExpression existsFalse =
          new ComparisonExpression(
              "f10",
              List.of(ValueComparisonOperation.build(ElementComparisonOperator.EXISTS, true)),
              null);
      final ComparisonExpression size =
          new ComparisonExpression(
              "f11",
              List.of(
                  ValueComparisonOperation.build(ArrayComparisonOperator.SIZE, new BigDecimal(-1))),
              null);
      assertThat(logicalExpression.logicalExpressions).hasSize(1);
      assertThat(logicalExpression.logicalExpressions.get(0).getLogicalRelation())
          .isEqualTo(LogicalExpression.LogicalOperator.OR);

      assertThat(logicalExpression.logicalExpressions.get(0).comparisonExpressions).hasSize(11);
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(0)
                  .getFilterOperations())
          .isEqualTo(eq.getFilterOperations());
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(1)
                  .getFilterOperations())
          .isEqualTo(ne.getFilterOperations());
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(2)
                  .getFilterOperations())
          .isEqualTo(in.getFilterOperations());
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(3)
                  .getFilterOperations())
          .isEqualTo(nin.getFilterOperations());
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(4)
                  .getFilterOperations())
          .isEqualTo(lt.getFilterOperations());
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(5)
                  .getFilterOperations())
          .isEqualTo(lte.getFilterOperations());
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(6)
                  .getFilterOperations())
          .isEqualTo(gt.getFilterOperations());
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(7)
                  .getFilterOperations())
          .isEqualTo(gte.getFilterOperations());
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(8)
                  .getFilterOperations())
          .isEqualTo(existsTrue.getFilterOperations());
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(9)
                  .getFilterOperations())
          .isEqualTo(existsFalse.getFilterOperations());
      assertThat(
              logicalExpression
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
                                   "find": {
                                       "filter": {
                                              "$not":{ "$not" : {"name" : "testName"} }
                                       }
                                   }
                               }
                      """;
      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      final FilterClause filterClause = invertedFilterClause(commandContext, findCommand);
      final LogicalExpression logicalExpression = filterClause.logicalExpression();

      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "name",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, "testName")),
              null);
      assertThat(logicalExpression.logicalExpressions).hasSize(0);
      assertThat(logicalExpression.comparisonExpressions.size()).isEqualTo(1);
      assertThat(logicalExpression.comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
    }

    @Test
    public void multipleLevel() throws Exception {

      String json =
          """
                      {
                                       "find": {
                                           "filter": {
                                               "$and": [
                                                   {
                                                       "name": "testName"
                                                   },
                                                   {
                                                       "age": "testAge"
                                                   },
                                                   {
                                                       "$not": {
                                                           "$or": [
                                                               {
                                                                   "address": "testAddress"
                                                               },
                                                               {
                                                                   "tags": {
                                                                       "$size": 1
                                                                   }
                                                               }
                                                           ]
                                                       }
                                                   }
                                               ]
                                           }
                                       }
                                   }
                      """;
      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      final FilterClause filterClause = invertedFilterClause(commandContext, findCommand);
      final LogicalExpression logicalExpression = filterClause.logicalExpression();

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
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.NE, "testAddress")),
              null);
      final ComparisonExpression expectedResult4 =
          new ComparisonExpression(
              "tags",
              List.of(
                  ValueComparisonOperation.build(ArrayComparisonOperator.SIZE, new BigDecimal(-1))),
              null);
      assertThat(logicalExpression.logicalExpressions.get(0).getLogicalRelation())
          .isEqualTo(LogicalExpression.LogicalOperator.AND);

      assertThat(logicalExpression.logicalExpressions.get(0).comparisonExpressions).hasSize(2);
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(0)
                  .getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(1)
                  .getFilterOperations())
          .isEqualTo(expectedResult2.getFilterOperations());

      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .logicalExpressions
                  .get(0)
                  .getLogicalRelation())
          .isEqualTo(LogicalExpression.LogicalOperator.AND);
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions)
          .hasSize(2);
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(0)
                  .getFilterOperations())
          .isEqualTo(expectedResult3.getFilterOperations());
      assertThat(
              logicalExpression
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
    public void multipleLevelNot() throws Exception {

      String json =
          """
                                {
                                   "find": {
                                       "filter": {
                                            "$not": { "$not" : { "$not" : {"name" : "testName"} } }
                                          }
                                   }
                               }
                      """;
      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      final FilterClause filterClause = invertedFilterClause(commandContext, findCommand);
      final LogicalExpression logicalExpression = filterClause.logicalExpression();

      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "name",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.NE, "testName")),
              null);
      assertThat(logicalExpression.logicalExpressions).hasSize(0);
      assertThat(logicalExpression.comparisonExpressions.size()).isEqualTo(1);
      assertThat(logicalExpression.comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
    }

    @Test
    public void notLogicalOperatorOR() throws Exception {

      String json =
          """
                               {
                                   "find": {
                                       "filter": {
                                           "$not": {
                                               "$or": [
                                                   {
                                                       "address": "Shanghai"
                                                   },
                                                   {
                                                       "gender": "male"
                                                   }
                                               ]
                                           }
                                       }
                                   }
                               }
                      """;
      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      final FilterClause filterClause = invertedFilterClause(commandContext, findCommand);
      final LogicalExpression logicalExpression = filterClause.logicalExpression();

      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "address",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.NE, "Shanghai")),
              null);
      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "gender",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.NE, "male")),
              null);
      assertThat(logicalExpression.logicalExpressions).hasSize(1);
      assertThat(logicalExpression.comparisonExpressions.size()).isEqualTo(0);
      assertThat(logicalExpression.logicalExpressions.get(0).logicalExpressions).hasSize(0);
      assertThat(logicalExpression.logicalExpressions.get(0).getLogicalRelation())
          .isEqualTo(LogicalExpression.LogicalOperator.AND);
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(0)
                  .getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(1)
                  .getFilterOperations())
          .isEqualTo(expectedResult2.getFilterOperations());
    }

    @Test
    public void notLogicalOperatorAND() throws Exception {

      String json =
          """
                               {
                                   "find": {
                                       "filter": {
                                           "$not": {
                                               "$and": [
                                                   {
                                                       "address": "Shanghai"
                                                   },
                                                   {
                                                       "gender": "male"
                                                   }
                                               ]
                                           }
                                       }
                                   }
                               }
                      """;
      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      final FilterClause filterClause = invertedFilterClause(commandContext, findCommand);
      final LogicalExpression logicalExpression = filterClause.logicalExpression();

      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "address",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.NE, "Shanghai")),
              null);
      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "gender",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.NE, "male")),
              null);
      assertThat(logicalExpression.logicalExpressions).hasSize(1);
      assertThat(logicalExpression.comparisonExpressions.size()).isEqualTo(0);
      assertThat(logicalExpression.logicalExpressions.get(0).logicalExpressions).hasSize(0);
      assertThat(logicalExpression.logicalExpressions.get(0).getLogicalRelation())
          .isEqualTo(LogicalExpression.LogicalOperator.OR);
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(0)
                  .getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(1)
                  .getFilterOperations())
          .isEqualTo(expectedResult2.getFilterOperations());
    }

    @Test
    public void notMultipleLogicalOperators() throws Exception {

      String json =
          """
                         {
                             "find": {
                                 "filter": {
                                     "$not": {
                                         "$and": [
                                             {
                                                 "address": "Shanghai"
                                             },
                                             {
                                                 "gender": "male"
                                             }
                                         ],
                                         "$or": [
                                             {
                                                 "color": "yellow"
                                             },
                                             {
                                                 "height": 175
                                             }
                                         ]
                                     }
                                 }
                             }
                         }
                      """;
      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      final FilterClause filterClause = invertedFilterClause(commandContext, findCommand);
      final LogicalExpression logicalExpression = filterClause.logicalExpression();

      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "address",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.NE, "Shanghai")),
              null);
      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "gender",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.NE, "male")),
              null);
      final ComparisonExpression expectedResult3 =
          new ComparisonExpression(
              "color",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.NE, "yellow")),
              null);
      final ComparisonExpression expectedResult4 =
          new ComparisonExpression(
              "height",
              List.of(
                  ValueComparisonOperation.build(ValueComparisonOperator.NE, new BigDecimal(175))),
              null);
      assertThat(logicalExpression.logicalExpressions).hasSize(2);
      assertThat(logicalExpression.comparisonExpressions.size()).isEqualTo(0);
      assertThat(logicalExpression.logicalExpressions.get(0).logicalExpressions).hasSize(0);
      assertThat(logicalExpression.logicalExpressions.get(0).getLogicalRelation())
          .isEqualTo(LogicalExpression.LogicalOperator.OR);
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(0)
                  .getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(1)
                  .getFilterOperations())
          .isEqualTo(expectedResult2.getFilterOperations());
      assertThat(logicalExpression.logicalExpressions.get(1).logicalExpressions).hasSize(0);
      assertThat(logicalExpression.logicalExpressions.get(1).getLogicalRelation())
          .isEqualTo(LogicalExpression.LogicalOperator.AND);
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(1)
                  .comparisonExpressions
                  .get(0)
                  .getFilterOperations())
          .isEqualTo(expectedResult3.getFilterOperations());
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(1)
                  .comparisonExpressions
                  .get(1)
                  .getFilterOperations())
          .isEqualTo(expectedResult4.getFilterOperations());
    }

    @Test
    public void notLogicalOperatorWithComparisonExpression() throws Exception {

      String json =
          """
                       {
                           "find": {
                               "filter": {
                                   "$not": {
                                       "$and": [
                                           {
                                               "address": "Shanghai"
                                           },
                                           {
                                               "gender": "male"
                                           }
                                       ],
                                       "color": "yellow"
                                   }
                               }
                           }
                       }
                      """;
      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      final FilterClause filterClause = invertedFilterClause(commandContext, findCommand);
      final LogicalExpression logicalExpression = filterClause.logicalExpression();

      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "address",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.NE, "Shanghai")),
              null);
      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "gender",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.NE, "male")),
              null);
      final ComparisonExpression expectedResult3 =
          new ComparisonExpression(
              "color",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.NE, "yellow")),
              null);
      assertThat(logicalExpression.logicalExpressions).hasSize(1);
      assertThat(logicalExpression.comparisonExpressions.size()).isEqualTo(0);
      // generated OR relation after flipped what is in the $not
      final LogicalExpression logicalExpressionOR = logicalExpression.logicalExpressions.get(0);
      assertThat(logicalExpressionOR.getLogicalRelation())
          .isEqualTo(LogicalExpression.LogicalOperator.OR);
      assertThat(logicalExpressionOR.logicalExpressions).hasSize(1);
      assertThat(logicalExpressionOR.comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult3.getFilterOperations());

      // generated OR relation after flipped $and
      final LogicalExpression logicalExpressionAND = logicalExpressionOR.logicalExpressions.get(0);
      assertThat(logicalExpressionAND.comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
      assertThat(logicalExpressionAND.comparisonExpressions.get(1).getFilterOperations())
          .isEqualTo(expectedResult2.getFilterOperations());
    }

    @Test
    public void notMultipleComparisonExpression() throws Exception {

      String json =
          """
                          {
                              "find": {
                                  "filter": {
                                      "age": 25,
                                      "$not": {
                                          "address": "Shanghai",
                                          "gender": "male"
                                      }
                                  }
                              }
                          }
                      """;
      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      final FilterClause filterClause = invertedFilterClause(commandContext, findCommand);
      final LogicalExpression logicalExpression = filterClause.logicalExpression();

      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "address",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.NE, "Shanghai")),
              null);
      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "gender",
              List.of(ValueComparisonOperation.build(ValueComparisonOperator.NE, "male")),
              null);
      final ComparisonExpression expectedResult3 =
          new ComparisonExpression(
              "height",
              List.of(
                  ValueComparisonOperation.build(ValueComparisonOperator.EQ, new BigDecimal(25))),
              null);
      assertThat(logicalExpression.logicalExpressions).hasSize(1);
      assertThat(logicalExpression.comparisonExpressions.size()).isEqualTo(1);
      assertThat(logicalExpression.comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult3.getFilterOperations());

      assertThat(logicalExpression.getLogicalRelation())
          .isEqualTo(LogicalExpression.LogicalOperator.AND);
      assertThat(logicalExpression.logicalExpressions.get(0).logicalExpressions).hasSize(0);
      assertThat(logicalExpression.logicalExpressions.get(0).getLogicalRelation())
          .isEqualTo(LogicalExpression.LogicalOperator.OR);
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(0)
                  .getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
      assertThat(
              logicalExpression
                  .logicalExpressions
                  .get(0)
                  .comparisonExpressions
                  .get(1)
                  .getFilterOperations())
          .isEqualTo(expectedResult2.getFilterOperations());
    }
  }

  @Nested
  public class TableTest {

    public static final TestData TEST_DATA = new TestData();
    public static final TableSchemaObject TABLE_SCHEMA_OBJECT =
        TEST_DATA.schemaObject().tableWithMapSetList();
    public static final CommandContext<?> TABLE_COMMAND_CONTEXT =
        TEST_DATA.commandContext().tableSchemaObjectCommandContext(TABLE_SCHEMA_OBJECT);

    private static Stream<Arguments> invertedOperators() {
      return Stream.of(
          Arguments.of(ValueComparisonOperator.NIN, ValueComparisonOperator.IN),
          Arguments.of(ValueComparisonOperator.IN, ValueComparisonOperator.NIN),
          Arguments.of(ArrayComparisonOperator.ALL, ArrayComparisonOperator.NOTANY));
    }

    @ParameterizedTest
    @MethodSource("invertedOperators")
    public void invertFilterOperatorsForList(FilterOperator from, FilterOperator to)
        throws Exception {
      String json =
              """
                                {
                                   "find": {
                                       "filter": {
                                          "$not": {
                                            "%s" : {
                                                   "%s": ["value1","value2"]
                                             }
                                         }
                                      }
                                   }
                               }
                      """
              .formatted(TEST_DATA.names.CQL_LIST_COLUMN.asInternal(), from.getOperator());
      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      final FilterClause filterClause = invertedFilterClause(TABLE_COMMAND_CONTEXT, findCommand);
      final LogicalExpression logicalExpression = filterClause.logicalExpression();
      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              TEST_DATA.names.CQL_LIST_COLUMN.asInternal(),
              List.of(
                  ValueComparisonOperation.build(
                      to, List.of("value1", "value2"), MapSetListComponent.LIST_VALUE)),
              null);
      assertThat(logicalExpression.logicalExpressions).hasSize(0);
      assertThat(logicalExpression.comparisonExpressions.size()).isEqualTo(1);
      assertThat(logicalExpression.comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
    }

    @ParameterizedTest
    @MethodSource("invertedOperators")
    public void invertFilterOperatorsForSet(FilterOperator from, FilterOperator to)
        throws Exception {
      String json =
              """
                                {
                                   "find": {
                                       "filter": {
                                          "$not": {
                                            "%s" : {
                                                   "%s": ["value1","value2"]
                                             }
                                         }
                                      }
                                   }
                               }
                      """
              .formatted(TEST_DATA.names.CQL_SET_COLUMN.asInternal(), from.getOperator());
      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      final FilterClause filterClause = invertedFilterClause(TABLE_COMMAND_CONTEXT, findCommand);
      final LogicalExpression logicalExpression = filterClause.logicalExpression();
      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              TEST_DATA.names.CQL_SET_COLUMN.asInternal(),
              List.of(
                  ValueComparisonOperation.build(
                      to, List.of("value1", "value2"), MapSetListComponent.SET_VALUE)),
              null);
      assertThat(logicalExpression.logicalExpressions).hasSize(0);
      assertThat(logicalExpression.comparisonExpressions.size()).isEqualTo(1);
      assertThat(logicalExpression.comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
    }

    private static Stream<Arguments> invertedOperatorsMapKeyValues() {
      return Stream.of(
          Arguments.of(ValueComparisonOperator.NIN, ValueComparisonOperator.IN, "$keys"),
          Arguments.of(ValueComparisonOperator.IN, ValueComparisonOperator.NIN, "$keys"),
          Arguments.of(ArrayComparisonOperator.ALL, ArrayComparisonOperator.NOTANY, "$keys"),
          Arguments.of(ValueComparisonOperator.NIN, ValueComparisonOperator.IN, "$values"),
          Arguments.of(ValueComparisonOperator.IN, ValueComparisonOperator.NIN, "$values"),
          Arguments.of(ArrayComparisonOperator.ALL, ArrayComparisonOperator.NOTANY, "$values"));
    }

    @ParameterizedTest
    @MethodSource("invertedOperatorsMapKeyValues")
    public void invertFilterOperatorsForMapKeysOrValues(
        FilterOperator from, FilterOperator to, String keysOrMaps) throws Exception {
      String json =
              """
                            {
                                "find": {
                                    "filter": {
                                        "$not": {
                                            "%s": {
                                                "%s": {
                                                    "%s": [
                                                        "element1",
                                                        "element2"
                                                    ]
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                      """
              .formatted(
                  TEST_DATA.names.CQL_MAP_COLUMN.asInternal(), keysOrMaps, from.getOperator());
      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      final FilterClause filterClause = invertedFilterClause(TABLE_COMMAND_CONTEXT, findCommand);
      final LogicalExpression logicalExpression = filterClause.logicalExpression();
      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              TEST_DATA.names.CQL_MAP_COLUMN.asInternal(),
              List.of(
                  ValueComparisonOperation.build(
                      to,
                      List.of("element1", "element2"),
                      MapSetListComponent.fromMapComponent(
                          MapComponentDesc.fromApiName(keysOrMaps).get()))),
              null);
      assertThat(logicalExpression.logicalExpressions).hasSize(0);
      assertThat(logicalExpression.comparisonExpressions.size()).isEqualTo(1);
      assertThat(logicalExpression.comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
    }

    @ParameterizedTest
    @MethodSource("invertedOperators")
    public void invertFilterOperatorsForMapEntries(FilterOperator from, FilterOperator to)
        throws Exception {
      String json =
              """
                            {
                                "find": {
                                    "filter": {
                                        "$not": {
                                            "%s": {
                                                "%s": [
                                                    [
                                                        "key1",
                                                        "value1"
                                                    ],
                                                    [
                                                        "key2",
                                                        "value2"
                                                    ]
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                      """
              .formatted(TEST_DATA.names.CQL_MAP_COLUMN.asInternal(), from.getOperator());
      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      final FilterClause filterClause = invertedFilterClause(TABLE_COMMAND_CONTEXT, findCommand);
      final LogicalExpression logicalExpression = filterClause.logicalExpression();
      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              TEST_DATA.names.CQL_MAP_COLUMN.asInternal(),
              List.of(
                  ValueComparisonOperation.build(
                      to,
                      List.of(List.of("key1", "value1"), List.of("key2", "value2")),
                      MapSetListComponent.MAP_ENTRY)),
              null);
      assertThat(logicalExpression.logicalExpressions).hasSize(0);
      assertThat(logicalExpression.comparisonExpressions.size()).isEqualTo(1);
      assertThat(logicalExpression.comparisonExpressions.get(0).getFilterOperations())
          .isEqualTo(expectedResult1.getFilterOperations());
    }
  }

  private <CMD extends Command & Filterable> FilterClause invertedFilterClause(
      CommandContext<?> ctx, CMD cmd) {
    // Building calls invert method so nothing else to do here
    return cmd.filterClause(ctx);
  }
}
