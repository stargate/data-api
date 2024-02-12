package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FindCommandResolverTest {
  @Inject ObjectMapper objectMapper;
  @Inject OperationsConfig operationsConfig;
  @Inject FindCommandResolver resolver;

  @Nested
  class FindCommandResolveCommand {

    CommandContext commandContext = CommandContext.empty();

    @Test
    public void idFilterCondition() throws Exception {
      String json =
          """
          {
            "find": {
              "filter" : {"_id" : "id"}
            }
          }
          """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.IDFilter(
                        DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("id"));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isEqualTo(filter);
              });
    }

    @Test
    public void dateFilterCondition() throws Exception {
      String json =
          """
          {
            "find": {
              "filter" : {"date_field" : {"$date" : 1672531200000}}
            }
          }
          """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.DateFilter(
                        "date_field",
                        DBFilterBase.MapFilterBase.Operator.EQ,
                        new Date(1672531200000L));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isEqualTo(filter);
              });
    }

    @Test
    public void idFilterWithInOperatorCondition() throws Exception {
      String json =
          """
          {
            "find": {
              "filter" : {"_id" : { "$in" : ["id1", "id2"]}}
            }
          }
          """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.IDFilter(
                        DBFilterBase.IDFilter.Operator.IN,
                        List.of(DocumentId.fromString("id1"), DocumentId.fromString("id2")));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isEqualTo(filter);
              });
    }

    @Test
    public void idFilterWithInOperatorEmptyArrayCondition() throws Exception {
      String json =
          """
          {
            "find": {
              "filter" : {"_id" : { "$in" : []}}
            }
          }
          """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.IDFilter(DBFilterBase.IDFilter.Operator.IN, List.of());

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isEqualTo(filter);
              });
    }

    @Test
    public void arraySizeFilter() throws Exception {
      String json =
          """
          {
            "find": {
              "filter" : {"tags" : { "$size" : 0}}
            }
          }
          """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.SizeFilter(
                        "tags", DBFilterBase.MapFilterBase.Operator.MAP_EQUALS, 0);
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isEqualTo(filter);
              });
    }

    @Test
    public void byIdInAndOtherConditionTogether() throws Exception {
      String json =
          """
          {
            "find": {
              "filter" : {"_id" : { "$in" : ["id1", "id2"]}, "field1" : "value1" }
            }
          }
          """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.IDFilter(
                        DBFilterBase.IDFilter.Operator.IN,
                        List.of(DocumentId.fromString("id1"), DocumentId.fromString("id2")));
                DBFilterBase filter2 =
                    new DBFilterBase.TextFilter(
                        "field1", DBFilterBase.TextFilter.Operator.EQ, "value1");

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isIn(filter, filter2);
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(1).getDbFilters().get(0))
                    .isIn(filter, filter2);
              });
    }

    @Test
    public void noFilterCondition() throws Exception {
      String json = """
          {
            "find": {
            }
          }
          """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.logicalExpression().comparisonExpressions).isEmpty();
              });
    }

    @Test
    public void sort() throws Exception {
      String json =
          """
          {
            "find": {
              "sort" : {"username" : 1}
            }
          }
          """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                FindOperation.OrderBy orderBy = new FindOperation.OrderBy("username", true);

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultSortPageSize());
                assertThat(find.limit()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.SORTED_DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit())
                    .isEqualTo(operationsConfig.maxDocumentSortCount());
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).containsOnly(orderBy);
                assertThat(find.logicalExpression().comparisonExpressions).isEmpty();
              });
    }

    @Test
    public void sortDesc() throws Exception {
      String json =
          """
          {
            "find": {
              "sort" : {"username" : -1}
            }
          }
          """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                FindOperation.OrderBy orderBy = new FindOperation.OrderBy("username", false);

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultSortPageSize());
                assertThat(find.limit()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.SORTED_DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit())
                    .isEqualTo(operationsConfig.maxDocumentSortCount());
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).containsOnly(orderBy);
                assertThat(find.logicalExpression().comparisonExpressions).isEmpty();
              });
    }

    @Test
    public void vectorSearch() throws Exception {
      String json =
          """
          {
            "find": {
              "sort" : {"$vector" : [0.11, 0.22, 0.33, 0.44]}
            }
          }
          """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                float[] vector = new float[] {0.11f, 0.22f, 0.33f, 0.44f};
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(operationsConfig.maxVectorSearchLimit());
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.vector()).containsExactly(vector);
                assertThat(find.logicalExpression().comparisonExpressions).isEmpty();
              });
    }

    @Test
    public void vectorSearchWithOptionSimilarity() throws Exception {
      String json =
          """
                      {
                        "find": {
                          "sort" : {"$vector" : [0.11, 0.22, 0.33, 0.44]},
                          "options": {"includeSimilarity": true}
                        }
                      }
                      """;
      final DocumentProjector projector = DocumentProjector.createFromDefinition(null, true);

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                float[] vector = new float[] {0.11f, 0.22f, 0.33f, 0.44f};
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(projector);
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(operationsConfig.maxVectorSearchLimit());
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.vector()).containsExactly(vector);
                assertThat(find.logicalExpression().comparisonExpressions).isEmpty();
              });
    }

    @Test
    public void vectorSearchWithFilter() throws Exception {
      String json =
          """
          {
            "find": {
              "filter" : {"_id" : "id"},
              "sort" : {"$vector" : [0.11, 0.22, 0.33, 0.44]}
            }
          }
          """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.IDFilter(
                        DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("id"));
                float[] vector = new float[] {0.11f, 0.22f, 0.33f, 0.44f};
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(operationsConfig.maxVectorSearchLimit());
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.vector()).containsExactly(vector);
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isEqualTo(filter);
              });
    }

    @Test
    public void noFilterConditionSortAndOptions() throws Exception {
      String json =
          """
          {
            "find": {
              "sort" : {"username" : 1},
              "options" : {"skip" : 5, "limit" : 10}
            }
          }
          """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                FindOperation.OrderBy orderBy = new FindOperation.OrderBy("username", true);

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultSortPageSize());
                assertThat(find.limit()).isEqualTo(10);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.SORTED_DOCUMENT);
                assertThat(find.skip()).isEqualTo(5);
                assertThat(find.maxSortReadLimit())
                    .isEqualTo(operationsConfig.maxDocumentSortCount());
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).containsOnly(orderBy);
                assertThat(find.logicalExpression().comparisonExpressions).isEmpty();
              });
    }

    @Test
    public void noFilterConditionWithOptions() throws Exception {
      String json =
          """
              {
                "find": {
                  "options" : {
                    "limit" : 7,
                    "pageState" : "dlavjhvbavkjbna"
                  }
                }
              }
              """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(7);
                assertThat(find.pageState()).isEqualTo("dlavjhvbavkjbna");
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.logicalExpression().comparisonExpressions).isEmpty();
              });
    }

    @Test
    public void dynamicFilterCondition() throws Exception {
      String json =
          """
          {
            "find": {
              "filter" : {"col" : "val"}
            }
          }
          """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.TextFilter(
                        "col", DBFilterBase.MapFilterBase.Operator.EQ, "val");

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isEqualTo(filter);
              });
    }

    @Test
    public void explicitAnd() throws Exception {
      String json =
          """
                          {
                            "find": {
                              "filter" :{
                                "$and":[
                                    {"name" : "testName"},
                                    {"age" : "testAge"}
                                 ]
                              }
                            }
                          }
                          """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);
      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter1 =
                    new DBFilterBase.TextFilter(
                        "name", DBFilterBase.TextFilter.Operator.EQ, "testName");
                DBFilterBase filter2 =
                    new DBFilterBase.TextFilter(
                        "age", DBFilterBase.TextFilter.Operator.EQ, "testAge");
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.logicalExpression().logicalExpressions.get(0).getLogicalRelation())
                    .isEqualTo(LogicalExpression.LogicalOperator.AND);
                assertThat(find.logicalExpression().getTotalComparisonExpressionCount())
                    .isEqualTo(2);
                assertThat(
                        find.logicalExpression()
                            .logicalExpressions
                            .get(0)
                            .comparisonExpressions
                            .get(0)
                            .getDbFilters()
                            .get(0))
                    .isEqualTo(filter1);
                assertThat(
                        find.logicalExpression()
                            .logicalExpressions
                            .get(0)
                            .comparisonExpressions
                            .get(1)
                            .getDbFilters()
                            .get(0))
                    .isEqualTo(filter2);
              });
    }

    @Test
    public void explicitOr() throws Exception {
      String json =
          """
                          {
                            "find": {
                              "filter" :{
                                "$or":[
                                    {"name" : "testName"},
                                    {"age" : "testAge"}
                                 ]
                              }
                            }
                          }
                          """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);
      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter1 =
                    new DBFilterBase.TextFilter(
                        "name", DBFilterBase.TextFilter.Operator.EQ, "testName");
                DBFilterBase filter2 =
                    new DBFilterBase.TextFilter(
                        "age", DBFilterBase.TextFilter.Operator.EQ, "testAge");
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.logicalExpression().logicalExpressions.get(0).getLogicalRelation())
                    .isEqualTo(LogicalExpression.LogicalOperator.OR);
                assertThat(find.logicalExpression().getTotalComparisonExpressionCount())
                    .isEqualTo(2);
                assertThat(
                        find.logicalExpression()
                            .logicalExpressions
                            .get(0)
                            .comparisonExpressions
                            .get(0)
                            .getDbFilters()
                            .get(0))
                    .isEqualTo(filter1);
                assertThat(
                        find.logicalExpression()
                            .logicalExpressions
                            .get(0)
                            .comparisonExpressions
                            .get(1)
                            .getDbFilters()
                            .get(0))
                    .isEqualTo(filter2);
              });
    }

    @Test
    public void emptyAnd() throws Exception {
      String json =
          """
                          {
                            "find": {
                              "filter" :{
                                "$and":[
                                 ]
                              }
                            }
                          }
                          """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);
      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.logicalExpression().logicalExpressions).isEmpty();
                assertThat(find.logicalExpression().comparisonExpressions).isEmpty();
              });
    }

    @Test
    public void nestedAndOr() throws Exception {
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
                              }
                          }
                          """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);
      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter1 =
                    new DBFilterBase.TextFilter(
                        "name", DBFilterBase.TextFilter.Operator.EQ, "testName");
                DBFilterBase filter2 =
                    new DBFilterBase.TextFilter(
                        "age", DBFilterBase.TextFilter.Operator.EQ, "testAge");
                DBFilterBase filter3 =
                    new DBFilterBase.TextFilter(
                        "address", DBFilterBase.TextFilter.Operator.EQ, "testAddress");
                DBFilterBase filter4 =
                    new DBFilterBase.TextFilter(
                        "height", DBFilterBase.TextFilter.Operator.EQ, "testHeight");
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.logicalExpression().logicalExpressions.get(0).getLogicalRelation())
                    .isEqualTo(LogicalExpression.LogicalOperator.AND);
                assertThat(find.logicalExpression().getTotalComparisonExpressionCount())
                    .isEqualTo(4);
                assertThat(
                        find.logicalExpression()
                            .logicalExpressions
                            .get(0)
                            .comparisonExpressions
                            .get(0)
                            .getDbFilters()
                            .get(0))
                    .isEqualTo(filter1);
                assertThat(
                        find.logicalExpression()
                            .logicalExpressions
                            .get(0)
                            .comparisonExpressions
                            .get(1)
                            .getDbFilters()
                            .get(0))
                    .isEqualTo(filter2);

                assertThat(
                        find.logicalExpression()
                            .logicalExpressions
                            .get(0)
                            .logicalExpressions
                            .get(0)
                            .getLogicalRelation())
                    .isEqualTo(LogicalExpression.LogicalOperator.OR);
                assertThat(
                        find.logicalExpression()
                            .logicalExpressions
                            .get(0)
                            .logicalExpressions
                            .get(0)
                            .comparisonExpressions
                            .get(0)
                            .getDbFilters()
                            .get(0))
                    .isEqualTo(filter3);
                assertThat(
                        find.logicalExpression()
                            .logicalExpressions
                            .get(0)
                            .logicalExpressions
                            .get(0)
                            .comparisonExpressions
                            .get(1)
                            .getDbFilters()
                            .get(0))
                    .isEqualTo(filter4);
              });
    }

    @Test
    public void emptyOr() throws Exception {
      String json =
          """
                          {
                            "find": {
                              "filter" :{
                                "$or":[
                                 ]
                              }
                            }
                          }
                          """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);
      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.logicalExpression().logicalExpressions).isEmpty();
                assertThat(find.logicalExpression().comparisonExpressions).isEmpty();
              });
    }
  }

  @Nested
  class FindCommandResolveWithProjection {

    CommandContext commandContext = CommandContext.empty();

    @Test
    public void idFilterConditionAndProjection() throws Exception {
      final String json =
          """
          {
            "find": {
              "filter" : {"_id" : "id"},
              "projection": {
                "field1" : 1,
                "field2" : 1
              }
            }
          }
          """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      JsonNode projectionDef =
          objectMapper.readTree(
              """
                      {
                        "field1" : 1,
                        "field2" : 1
                      }
                      """);
      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.IDFilter(
                        DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("id"));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection())
                    .isEqualTo(DocumentProjector.createFromDefinition(projectionDef));
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isEqualTo(filter);
              });
    }

    @Test
    public void noFilterConditionWithProjection() throws Exception {
      final String json =
          """
              {
                "find": {
                  "projection": {
                    "field1" : 1,
                    "field2" : 1
                  }
                }
              }
              """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      JsonNode projectionDef =
          objectMapper.readTree(
              """
                      {
                        "field1" : 1,
                        "field2" : 1
                      }
                      """);
      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection())
                    .isEqualTo(DocumentProjector.createFromDefinition(projectionDef));
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.logicalExpression().comparisonExpressions).isEmpty();
              });
    }
  }

  @Nested
  class FindCommandResolveWithINOperator {
    CommandContext commandContext = CommandContext.empty();

    @Test
    public void NonIdIn() throws Exception {
      String json =
          """
                    {
                      "find": {
                        "filter" : {"name" : { "$in" : ["test1", "test2"]}}
                      }
                    }
                    """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.InFilter(
                        DBFilterBase.InFilter.Operator.IN, "name", List.of("test1", "test2"));
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isEqualTo(filter);
              });

      final FindOperation operation1 = (FindOperation) operation;
    }

    @Test
    public void NonIdInIdEq() throws Exception {
      String json =
          """
                    {
                      "find": {
                        "filter" : {
                        "_id" : "id1",
                        "name" : { "$in" : ["test1", "test2"]}
                        }
                      }
                    }
                    """;
      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);
      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase inFilter =
                    new DBFilterBase.InFilter(
                        DBFilterBase.InFilter.Operator.IN, "name", List.of("test1", "test2"));
                DBFilterBase idFilter =
                    new DBFilterBase.IDFilter(
                        DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("id1"));
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isIn(inFilter, idFilter);
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(1).getDbFilters().get(0))
                    .isIn(inFilter, idFilter);
              });
    }

    @Test
    public void NonIdInIdIn() throws Exception {
      String json =
          """
                    {
                      "find": {
                        "filter" : {
                        "_id" : { "$in" : ["id1", "id2"]},
                        "name" : { "$in" : ["test1", "test2"]}
                        }
                      }
                    }
                    """;
      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);
      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase inFilter =
                    new DBFilterBase.InFilter(
                        DBFilterBase.InFilter.Operator.IN, "name", List.of("test1", "test2"));
                DBFilterBase idFilter =
                    new DBFilterBase.IDFilter(
                        DBFilterBase.IDFilter.Operator.IN,
                        List.of(DocumentId.fromString("id1"), DocumentId.fromString("id2")));
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isIn(inFilter, idFilter);
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(1).getDbFilters().get(0))
                    .isIn(inFilter, idFilter);
              });
    }

    @Test
    public void NonIdInVSearch() throws Exception {
      String json =
          """
                    {
                      "find": {
                        "filter" : {
                            "name" : { "$in" : ["test1", "test2"]}
                        },
                        "sort" : {"$vector" : [0.15, 0.1, 0.1]}
                      }
                    }
                    """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase inFilter =
                    new DBFilterBase.InFilter(
                        DBFilterBase.InFilter.Operator.IN, "name", List.of("test1", "test2"));
                float[] vector = new float[] {0.15f, 0.1f, 0.1f};
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(operationsConfig.maxVectorSearchLimit());
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.vector()).containsExactly(vector);
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isEqualTo(inFilter);
              });
    }

    @Test
    public void NonIdInIdInVSearch() throws Exception {
      String json =
          """
                    {
                      "find": {
                        "filter" : {
                            "_id" : { "$in" : ["id1", "id2"]},
                            "name" : { "$in" : ["test1", "test2"]}
                        },
                        "sort" : {"$vector" : [0.15, 0.1, 0.1]}
                      }
                    }
                    """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase inFilter =
                    new DBFilterBase.InFilter(
                        DBFilterBase.InFilter.Operator.IN, "name", List.of("test1", "test2"));
                DBFilterBase idFilter =
                    new DBFilterBase.IDFilter(
                        DBFilterBase.IDFilter.Operator.IN,
                        List.of(DocumentId.fromString("id1"), DocumentId.fromString("id2")));
                float[] vector = new float[] {0.15f, 0.1f, 0.1f};
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(operationsConfig.maxVectorSearchLimit());
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.vector()).containsExactly(vector);
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isIn(inFilter, idFilter);
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(1).getDbFilters().get(0))
                    .isIn(inFilter, idFilter);
              });
    }

    @Test
    public void descendingSortNonIdIn() throws Exception {
      String json =
          """
                        {
                            "find": {
                                "sort": {
                                    "name": -1
                                },
                                "filter" : {
                                    "name" : {"$in" : ["test1", "test2"]}
                                }
                            }
                        }
                    """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                FindOperation.OrderBy orderBy = new FindOperation.OrderBy("name", false);
                DBFilterBase inFilter =
                    new DBFilterBase.InFilter(
                        DBFilterBase.InFilter.Operator.IN, "name", List.of("test1", "test2"));
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultSortPageSize());
                assertThat(find.limit()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.SORTED_DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit())
                    .isEqualTo(operationsConfig.maxDocumentSortCount());
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).containsOnly(orderBy);
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isEqualTo(inFilter);
              });
    }

    @Test
    public void ascendingSortNonIdInIdIn() throws Exception {
      String json =
          """
                        {
                            "find": {
                                "sort": {
                                    "name": 1
                                },
                                "filter" : {
                                    "name" : {"$in" : ["test1", "test2"]},
                                    "_id" : {"$in" : ["id1","id2"]}
                                }
                            }
                        }
                    """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                FindOperation.OrderBy orderBy = new FindOperation.OrderBy("name", true);
                DBFilterBase inFilter =
                    new DBFilterBase.InFilter(
                        DBFilterBase.InFilter.Operator.IN, "name", List.of("test1", "test2"));
                DBFilterBase idFilter =
                    new DBFilterBase.IDFilter(
                        DBFilterBase.IDFilter.Operator.IN,
                        List.of(DocumentId.fromString("id1"), DocumentId.fromString("id2")));
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultSortPageSize());
                assertThat(find.limit()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.SORTED_DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit())
                    .isEqualTo(operationsConfig.maxDocumentSortCount());
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).containsOnly(orderBy);
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isIn(inFilter, idFilter);
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(1).getDbFilters().get(0))
                    .isIn(inFilter, idFilter);
              });
    }
  }

  @Nested
  class FindCommandResolveWithRangeOperator {
    CommandContext commandContext = CommandContext.empty();

    @Test
    public void gt() throws Exception {
      String json =
          """
            {
              "find": {
                "filter" : {"amount" : { "$gt" : 100}}
              }
            }
            """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.NumberFilter(
                        "amount", DBFilterBase.MapFilterBase.Operator.GT, new BigDecimal(100));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isEqualTo(filter);
              });
    }

    @Test
    public void gte() throws Exception {
      String json =
          """
            {
              "find": {
                "filter" : {"amount" : { "$gte" : 100}}
              }
            }
            """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.NumberFilter(
                        "amount", DBFilterBase.MapFilterBase.Operator.GTE, new BigDecimal(100));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isEqualTo(filter);
              });
    }

    @Test
    public void lt() throws Exception {
      String json =
          """
            {
              "find": {
                "filter" : {"amount" : { "$lt" : 100}}
              }
            }
            """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.NumberFilter(
                        "amount", DBFilterBase.MapFilterBase.Operator.LT, new BigDecimal(100));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isEqualTo(filter);
              });
    }

    @Test
    public void lte() throws Exception {
      String json =
          """
            {
              "find": {
                "filter" : {"dob": {"$lte" : {"$date" : 1672531200000}}}
              }
            }
            """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.DateFilter(
                        "dob", DBFilterBase.MapFilterBase.Operator.LTE, new Date(1672531200000L));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isEqualTo(filter);
              });
    }

    @Test
    public void rangeWithIdNumber() throws Exception {
      String json =
          """
        {
          "find": {
            "filter" : {"_id": {"$lte" : 5}}
          }
        }
        """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.NumberFilter(
                        "_id", DBFilterBase.MapFilterBase.Operator.LTE, new BigDecimal(5));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isEqualTo(filter);
              });
    }

    @Test
    public void rangeWithDateId() throws Exception {
      String json =
          """
        {
          "find": {
            "filter" : {"_id": {"$lte" : {"$date" : 1672531200000}}}
          }
        }
        """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase filter =
                    new DBFilterBase.DateFilter(
                        "_id", DBFilterBase.MapFilterBase.Operator.LTE, new Date(1672531200000L));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isEqualTo(filter);
              });
    }
  }
}
