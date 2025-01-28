package io.stargate.sgv2.jsonapi.service.resolver.matcher;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.InvertibleCommandClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.*;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class InvertibleFilterResolverTest {

  @Inject ObjectMapper objectMapper;
  @Inject OperationsConfig operationsConfig;

  @Nested
  public class CollectionTest {
    CommandContext<CollectionSchemaObject> commandContext = TestConstants.collectionContext();

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
      final FilterClause filterClause =
          commandContext.resolveFilterClause(findCommand.filterSpec());
      final LogicalExpression logicalExpression = filterClause.logicalExpression();
      InvertibleCommandClause.maybeInvert(commandContext, filterClause);
      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "name",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE, new JsonLiteral("testName", JsonType.STRING))),
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
      final FilterClause filterClause =
          commandContext.resolveFilterClause(findCommand.filterSpec());
      final LogicalExpression logicalExpression = filterClause.logicalExpression();
      InvertibleCommandClause.maybeInvert(commandContext, filterClause);
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
      final FilterClause filterClause =
          commandContext.resolveFilterClause(findCommand.filterSpec());
      final LogicalExpression logicalExpression = filterClause.logicalExpression();
      InvertibleCommandClause.maybeInvert(commandContext, filterClause);

      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "name",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("testName", JsonType.STRING))),
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
      final FilterClause filterClause =
          commandContext.resolveFilterClause(findCommand.filterSpec());
      final LogicalExpression logicalExpression = filterClause.logicalExpression();
      InvertibleCommandClause.maybeInvert(commandContext, filterClause);

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
      final FilterClause filterClause =
          commandContext.resolveFilterClause(findCommand.filterSpec());
      final LogicalExpression logicalExpression = filterClause.logicalExpression();
      InvertibleCommandClause.maybeInvert(commandContext, filterClause);

      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "name",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE, new JsonLiteral("testName", JsonType.STRING))),
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
      final FilterClause filterClause =
          commandContext.resolveFilterClause(findCommand.filterSpec());
      final LogicalExpression logicalExpression = filterClause.logicalExpression();
      InvertibleCommandClause.maybeInvert(commandContext, filterClause);

      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "address",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE, new JsonLiteral("Shanghai", JsonType.STRING))),
              null);
      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "gender",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE, new JsonLiteral("male", JsonType.STRING))),
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
      final FilterClause filterClause =
          commandContext.resolveFilterClause(findCommand.filterSpec());
      final LogicalExpression logicalExpression = filterClause.logicalExpression();
      InvertibleCommandClause.maybeInvert(commandContext, filterClause);

      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "address",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE, new JsonLiteral("Shanghai", JsonType.STRING))),
              null);
      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "gender",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE, new JsonLiteral("male", JsonType.STRING))),
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
      final FilterClause filterClause =
          commandContext.resolveFilterClause(findCommand.filterSpec());
      final LogicalExpression logicalExpression = filterClause.logicalExpression();
      InvertibleCommandClause.maybeInvert(commandContext, filterClause);

      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "address",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE, new JsonLiteral("Shanghai", JsonType.STRING))),
              null);
      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "gender",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE, new JsonLiteral("male", JsonType.STRING))),
              null);
      final ComparisonExpression expectedResult3 =
          new ComparisonExpression(
              "color",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE, new JsonLiteral("yellow", JsonType.STRING))),
              null);
      final ComparisonExpression expectedResult4 =
          new ComparisonExpression(
              "height",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE,
                      new JsonLiteral(new BigDecimal(175), JsonType.NUMBER))),
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
      final FilterClause filterClause =
          commandContext.resolveFilterClause(findCommand.filterSpec());
      final LogicalExpression logicalExpression = filterClause.logicalExpression();
      InvertibleCommandClause.maybeInvert(commandContext, filterClause);

      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "address",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE, new JsonLiteral("Shanghai", JsonType.STRING))),
              null);
      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "gender",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE, new JsonLiteral("male", JsonType.STRING))),
              null);
      final ComparisonExpression expectedResult3 =
          new ComparisonExpression(
              "color",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE, new JsonLiteral("yellow", JsonType.STRING))),
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
      final FilterClause filterClause =
          commandContext.resolveFilterClause(findCommand.filterSpec());
      final LogicalExpression logicalExpression = filterClause.logicalExpression();
      InvertibleCommandClause.maybeInvert(commandContext, filterClause);

      final ComparisonExpression expectedResult1 =
          new ComparisonExpression(
              "address",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE, new JsonLiteral("Shanghai", JsonType.STRING))),
              null);
      final ComparisonExpression expectedResult2 =
          new ComparisonExpression(
              "gender",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.NE, new JsonLiteral("male", JsonType.STRING))),
              null);
      final ComparisonExpression expectedResult3 =
          new ComparisonExpression(
              "height",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ,
                      new JsonLiteral(new BigDecimal(25), JsonType.NUMBER))),
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
  public class TableTest {}
}
