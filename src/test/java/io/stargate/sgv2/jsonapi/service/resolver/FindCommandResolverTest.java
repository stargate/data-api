package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.*;
import io.stargate.sgv2.jsonapi.service.operation.query.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
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
  @InjectMock protected DataApiRequestInfo dataApiRequestInfo;

  @Nested
  class FindCommandResolveCommand {

    CommandContext<CollectionSchemaObject> commandContext = TestConstants.COLLECTION_CONTEXT;

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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase filter =
                    new IDCollectionFilter(
                        IDCollectionFilter.Operator.EQ, DocumentId.fromString("id"));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase filter =
                    new DateCollectionFilter(
                        "date_field", MapCollectionFilter.Operator.EQ, new Date(1672531200000L));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase filter =
                    new IDCollectionFilter(
                        IDCollectionFilter.Operator.IN,
                        List.of(DocumentId.fromString("id1"), DocumentId.fromString("id2")));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase filter =
                    new IDCollectionFilter(IDCollectionFilter.Operator.IN, List.of());

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase filter =
                    new SizeCollectionFilter("tags", MapCollectionFilter.Operator.MAP_EQUALS, 0);
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                assertThat(find.includeSortVector()).isFalse();
              });
    }

    @Test
    public void nonVectorIncludeSortVector() throws Exception {
      String json =
          """
                  {
                    "find": {
                      "filter" : {"tags" : { "$size" : 0}},
                      "options" : {"includeSortVector" : true}
                    }
                  }
                  """;

      FindCommand findCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindCollectionOperation.class,
              find -> {
                DBFilterBase filter =
                    new SizeCollectionFilter("tags", MapCollectionFilter.Operator.MAP_EQUALS, 0);
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                assertThat(find.includeSortVector()).isTrue();
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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase filter =
                    new IDCollectionFilter(
                        IDCollectionFilter.Operator.IN,
                        List.of(DocumentId.fromString("id1"), DocumentId.fromString("id2")));
                DBFilterBase filter2 =
                    new TextCollectionFilter("field1", TextCollectionFilter.Operator.EQ, "value1");

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().filters().get(0)).isIn(filter, filter2);
                assertThat(find.dbLogicalExpression().filters().get(1)).isIn(filter, filter2);
              });
    }

    @Test
    public void noFilterCondition() throws Exception {
      String json =
          """
          {
            "find": {
            }
          }
          """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindCollectionOperation.class,
              find -> {
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().filters()).isEmpty();
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
              FindCollectionOperation.class,
              find -> {
                FindCollectionOperation.OrderBy orderBy =
                    new FindCollectionOperation.OrderBy("username", true);

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultSortPageSize());
                assertThat(find.limit()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.SORTED_DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit())
                    .isEqualTo(operationsConfig.maxDocumentSortCount());
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).containsOnly(orderBy);
                assertThat(find.dbLogicalExpression().filters()).isEmpty();
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
              FindCollectionOperation.class,
              find -> {
                FindCollectionOperation.OrderBy orderBy =
                    new FindCollectionOperation.OrderBy("username", false);

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultSortPageSize());
                assertThat(find.limit()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.SORTED_DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit())
                    .isEqualTo(operationsConfig.maxDocumentSortCount());
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).containsOnly(orderBy);
                assertThat(find.dbLogicalExpression().filters()).isEmpty();
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
              FindCollectionOperation.class,
              find -> {
                float[] vector = new float[] {0.11f, 0.22f, 0.33f, 0.44f};
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(operationsConfig.maxVectorSearchLimit());
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.vector()).containsExactly(vector);
                assertThat(find.dbLogicalExpression().filters()).isEmpty();
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
              FindCollectionOperation.class,
              find -> {
                float[] vector = new float[] {0.11f, 0.22f, 0.33f, 0.44f};
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(projector);
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(operationsConfig.maxVectorSearchLimit());
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.vector()).containsExactly(vector);
                assertThat(find.dbLogicalExpression().filters()).isEmpty();
                assertThat(find.includeSortVector()).isFalse();
              });
    }

    @Test
    public void vectorSearchWithOptionIncludeSortVector() throws Exception {
      String json =
          """
          {
            "find": {
              "sort" : {"$vector" : [0.11, 0.22, 0.33, 0.44]},
              "options": {"includeSortVector": true}
            }
          }
          """;
      final DocumentProjector projector = DocumentProjector.createFromDefinition(null, false);

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindCollectionOperation.class,
              find -> {
                float[] vector = new float[] {0.11f, 0.22f, 0.33f, 0.44f};
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(projector);
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(operationsConfig.maxVectorSearchLimit());
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.vector()).containsExactly(vector);
                assertThat(find.dbLogicalExpression().filters()).isEmpty();
                assertThat(find.includeSortVector()).isTrue();
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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase filter =
                    new IDCollectionFilter(
                        IDCollectionFilter.Operator.EQ, DocumentId.fromString("id"));
                float[] vector = new float[] {0.11f, 0.22f, 0.33f, 0.44f};
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(operationsConfig.maxVectorSearchLimit());
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.vector()).containsExactly(vector);
                assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
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
              FindCollectionOperation.class,
              find -> {
                FindCollectionOperation.OrderBy orderBy =
                    new FindCollectionOperation.OrderBy("username", true);

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultSortPageSize());
                assertThat(find.limit()).isEqualTo(10);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.SORTED_DOCUMENT);
                assertThat(find.skip()).isEqualTo(5);
                assertThat(find.maxSortReadLimit())
                    .isEqualTo(operationsConfig.maxDocumentSortCount());
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).containsOnly(orderBy);
                assertThat(find.dbLogicalExpression().filters()).isEmpty();
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
              FindCollectionOperation.class,
              find -> {
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(7);
                assertThat(find.pageState()).isEqualTo("dlavjhvbavkjbna");
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().filters()).isEmpty();
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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase filter =
                    new TextCollectionFilter("col", MapCollectionFilter.Operator.EQ, "val");

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase filter1 =
                    new TextCollectionFilter("name", TextCollectionFilter.Operator.EQ, "testName");
                DBFilterBase filter2 =
                    new TextCollectionFilter("age", TextCollectionFilter.Operator.EQ, "testAge");
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().subExpressions().get(0).operator())
                    .isEqualTo(DBLogicalExpression.DBLogicalOperator.AND);
                assertThat(find.dbLogicalExpression().totalFilterCount()).isEqualTo(2);
                assertThat(find.dbLogicalExpression().subExpressions().get(0).filters().get(0))
                    .isEqualTo(filter1);
                assertThat(find.dbLogicalExpression().subExpressions().get(0).filters().get(1))
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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase filter1 =
                    new TextCollectionFilter("name", TextCollectionFilter.Operator.EQ, "testName");
                DBFilterBase filter2 =
                    new TextCollectionFilter("age", TextCollectionFilter.Operator.EQ, "testAge");
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().subExpressions().get(0).operator())
                    .isEqualTo(DBLogicalExpression.DBLogicalOperator.OR);
                assertThat(find.dbLogicalExpression().totalFilterCount()).isEqualTo(2);
                assertThat(find.dbLogicalExpression().subExpressions().get(0).filters().get(0))
                    .isEqualTo(filter1);
                assertThat(find.dbLogicalExpression().subExpressions().get(0).filters().get(1))
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
              FindCollectionOperation.class,
              find -> {
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().subExpressions()).isEmpty();
                assertThat(find.dbLogicalExpression().filters()).isEmpty();
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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase filter1 =
                    new TextCollectionFilter("name", TextCollectionFilter.Operator.EQ, "testName");
                DBFilterBase filter2 =
                    new TextCollectionFilter("age", TextCollectionFilter.Operator.EQ, "testAge");
                DBFilterBase filter3 =
                    new TextCollectionFilter(
                        "address", TextCollectionFilter.Operator.EQ, "testAddress");
                DBFilterBase filter4 =
                    new TextCollectionFilter(
                        "height", TextCollectionFilter.Operator.EQ, "testHeight");
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().subExpressions().get(0).operator())
                    .isEqualTo(DBLogicalExpression.DBLogicalOperator.AND);
                assertThat(find.dbLogicalExpression().totalFilterCount()).isEqualTo(4);

                assertThat(find.dbLogicalExpression().subExpressions().get(0).filters().get(0))
                    .isEqualTo(filter1);
                assertThat(find.dbLogicalExpression().subExpressions().get(0).filters().get(1))
                    .isEqualTo(filter2);

                assertThat(
                        find.dbLogicalExpression()
                            .subExpressions()
                            .get(0)
                            .subExpressions()
                            .get(0)
                            .operator())
                    .isEqualTo(DBLogicalExpression.DBLogicalOperator.OR);

                assertThat(
                        find.dbLogicalExpression()
                            .subExpressions()
                            .get(0)
                            .subExpressions()
                            .get(0)
                            .filters()
                            .get(0))
                    .isEqualTo(filter3);
                assertThat(
                        find.dbLogicalExpression()
                            .subExpressions()
                            .get(0)
                            .subExpressions()
                            .get(0)
                            .filters()
                            .get(1))
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
              FindCollectionOperation.class,
              find -> {
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                // There is no filter within $or, so there is only one implicit and
                assertThat(find.dbLogicalExpression().subExpressions()).isEmpty();
                assertThat(find.dbLogicalExpression().operator())
                    .isEqualTo(DBLogicalExpression.DBLogicalOperator.AND);
                assertThat(find.dbLogicalExpression().filters()).isEmpty();
              });
    }
  }

  @Nested
  class FindCommandResolveWithProjection {

    CommandContext<CollectionSchemaObject> commandContext = TestConstants.COLLECTION_CONTEXT;

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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase filter =
                    new IDCollectionFilter(
                        IDCollectionFilter.Operator.EQ, DocumentId.fromString("id"));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection())
                    .isEqualTo(DocumentProjector.createFromDefinition(projectionDef));
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
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
              FindCollectionOperation.class,
              find -> {
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection())
                    .isEqualTo(DocumentProjector.createFromDefinition(projectionDef));
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().subExpressions()).isEmpty();
                // implicit and
                assertThat(find.dbLogicalExpression().operator())
                    .isEqualTo(DBLogicalExpression.DBLogicalOperator.AND);
                assertThat(find.dbLogicalExpression().filters()).isEmpty();
              });
    }
  }

  @Nested
  class FindCommandResolveWithINOperator {
    CommandContext<CollectionSchemaObject> commandContext = TestConstants.COLLECTION_CONTEXT;

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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase filter =
                    new InCollectionFilter(
                        InCollectionFilter.Operator.IN, "name", List.of("test1", "test2"));
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
              });

      final FindCollectionOperation operation1 = (FindCollectionOperation) operation;
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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase inFilter =
                    new InCollectionFilter(
                        InCollectionFilter.Operator.IN, "name", List.of("test1", "test2"));
                DBFilterBase idFilter =
                    new IDCollectionFilter(
                        IDCollectionFilter.Operator.EQ, DocumentId.fromString("id1"));
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().filters().get(0)).isIn(inFilter, idFilter);
                assertThat(find.dbLogicalExpression().filters().get(1)).isIn(inFilter, idFilter);
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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase inFilter =
                    new InCollectionFilter(
                        InCollectionFilter.Operator.IN, "name", List.of("test1", "test2"));
                DBFilterBase idFilter =
                    new IDCollectionFilter(
                        IDCollectionFilter.Operator.IN,
                        List.of(DocumentId.fromString("id1"), DocumentId.fromString("id2")));
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().filters().get(0)).isIn(inFilter, idFilter);
                assertThat(find.dbLogicalExpression().filters().get(1)).isIn(inFilter, idFilter);
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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase inFilter =
                    new InCollectionFilter(
                        InCollectionFilter.Operator.IN, "name", List.of("test1", "test2"));
                float[] vector = new float[] {0.15f, 0.1f, 0.1f};
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(operationsConfig.maxVectorSearchLimit());
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.vector()).containsExactly(vector);
                assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(inFilter);
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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase inFilter =
                    new InCollectionFilter(
                        InCollectionFilter.Operator.IN, "name", List.of("test1", "test2"));
                DBFilterBase idFilter =
                    new IDCollectionFilter(
                        IDCollectionFilter.Operator.IN,
                        List.of(DocumentId.fromString("id1"), DocumentId.fromString("id2")));
                float[] vector = new float[] {0.15f, 0.1f, 0.1f};
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(operationsConfig.maxVectorSearchLimit());
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.vector()).containsExactly(vector);
                assertThat(find.dbLogicalExpression().filters().get(0)).isIn(inFilter, idFilter);
                assertThat(find.dbLogicalExpression().filters().get(1)).isIn(inFilter, idFilter);
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
              FindCollectionOperation.class,
              find -> {
                FindCollectionOperation.OrderBy orderBy =
                    new FindCollectionOperation.OrderBy("name", false);
                DBFilterBase inFilter =
                    new InCollectionFilter(
                        InCollectionFilter.Operator.IN, "name", List.of("test1", "test2"));
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultSortPageSize());
                assertThat(find.limit()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.SORTED_DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit())
                    .isEqualTo(operationsConfig.maxDocumentSortCount());
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).containsOnly(orderBy);
                assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(inFilter);
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
              FindCollectionOperation.class,
              find -> {
                FindCollectionOperation.OrderBy orderBy =
                    new FindCollectionOperation.OrderBy("name", true);
                DBFilterBase inFilter =
                    new InCollectionFilter(
                        InCollectionFilter.Operator.IN, "name", List.of("test1", "test2"));
                DBFilterBase idFilter =
                    new IDCollectionFilter(
                        IDCollectionFilter.Operator.IN,
                        List.of(DocumentId.fromString("id1"), DocumentId.fromString("id2")));
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultSortPageSize());
                assertThat(find.limit()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.SORTED_DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit())
                    .isEqualTo(operationsConfig.maxDocumentSortCount());
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).containsOnly(orderBy);
                assertThat(find.dbLogicalExpression().filters().get(0)).isIn(inFilter, idFilter);
                assertThat(find.dbLogicalExpression().filters().get(1)).isIn(inFilter, idFilter);
              });
    }
  }

  @Nested
  class FindCommandResolveWithRangeOperator {
    CommandContext<CollectionSchemaObject> commandContext = TestConstants.COLLECTION_CONTEXT;

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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase filter =
                    new NumberCollectionFilter(
                        "amount", MapCollectionFilter.Operator.GT, new BigDecimal(100));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase filter =
                    new NumberCollectionFilter(
                        "amount", MapCollectionFilter.Operator.GTE, new BigDecimal(100));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase filter =
                    new NumberCollectionFilter(
                        "amount", MapCollectionFilter.Operator.LT, new BigDecimal(100));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase filter =
                    new DateCollectionFilter(
                        "dob", MapCollectionFilter.Operator.LTE, new Date(1672531200000L));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase filter =
                    new NumberCollectionFilter(
                        "_id", MapCollectionFilter.Operator.LTE, new BigDecimal(5));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
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
              FindCollectionOperation.class,
              find -> {
                DBFilterBase filter =
                    new DateCollectionFilter(
                        "_id", MapCollectionFilter.Operator.LTE, new Date(1672531200000L));

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isFalse();
                assertThat(find.orderBy()).isNull();
                assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
              });
    }
  }
}
