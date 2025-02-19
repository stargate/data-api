package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.IDCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.MapCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.TextCollectionFilter;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FindOneCommandResolverTest {
  @Inject ObjectMapper objectMapper;
  @Inject FindOneCommandResolver resolver;
  @InjectMock protected DataApiRequestInfo dataApiRequestInfo;

  @Nested
  class Resolve {

    CommandContext<CollectionSchemaObject> commandContext = TestConstants.collectionContext();

    @Test
    public void idFilterCondition() throws Exception {
      String json =
          """
                          {
                            "findOne": {
                              "filter" : {"_id" : "id"}
                            }
                          }
                          """;

      FindOneCommand command = objectMapper.readValue(json, FindOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindCollectionOperation.class,
              op -> {
                IDCollectionFilter filter =
                    new IDCollectionFilter(
                        IDCollectionFilter.Operator.EQ, DocumentId.fromString("id"));

                assertThat(op.objectMapper()).isEqualTo(objectMapper);
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.limit()).isEqualTo(1);
                assertThat(op.pageSize()).isEqualTo(1);
                assertThat(op.pageState()).isNull();
                assertThat(op.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(op.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                assertThat(op.orderBy()).isNull();
                assertThat(op.singleResponse()).isTrue();
              });
    }

    @Test
    public void filterConditionAndSort() throws Exception {
      String json =
          """
                          {
                            "findOne": {
                              "sort" : {"user.name" : 1, "user.age" : -1},
                              "filter" : {"status" : "active"}
                            }
                          }
                          """;

      FindOneCommand command = objectMapper.readValue(json, FindOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindCollectionOperation.class,
              op -> {
                TextCollectionFilter filter =
                    new TextCollectionFilter("status", MapCollectionFilter.Operator.EQ, "active");

                assertThat(op.objectMapper()).isEqualTo(objectMapper);
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.limit()).isEqualTo(1);
                assertThat(op.pageSize()).isEqualTo(100);
                assertThat(op.pageState()).isNull();
                assertThat(op.readType()).isEqualTo(CollectionReadType.SORTED_DOCUMENT);
                assertThat(op.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                assertThat(op.orderBy()).hasSize(2);
                assertThat(op.orderBy())
                    .isEqualTo(
                        List.of(
                            new FindCollectionOperation.OrderBy("user.name", true),
                            new FindCollectionOperation.OrderBy("user.age", false)));
                assertThat(op.singleResponse()).isTrue();
              });
    }

    @Test
    public void filterConditionAndVectorSearch() throws Exception {
      String json =
          """
              {
                "findOne": {
                  "sort" : {"$vector" : [0.11, 0.22, 0.33, 0.44]},
                  "filter" : {"status" : "active"}
                }
              }
              """;

      FindOneCommand command = objectMapper.readValue(json, FindOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindCollectionOperation.class,
              find -> {
                TextCollectionFilter filter =
                    new TextCollectionFilter("status", MapCollectionFilter.Operator.EQ, "active");

                float[] vector = new float[] {0.11f, 0.22f, 0.33f, 0.44f};
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(DocumentProjector.defaultProjector());
                assertThat(find.pageSize()).isEqualTo(1);
                assertThat(find.limit()).isEqualTo(1);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isTrue();
                assertThat(find.vector()).containsExactly(vector);
                assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
              });
    }

    @Test
    public void filterConditionAndVectorSearchWithOptionSimilarity() throws Exception {
      String json =
          """
                      {
                        "findOne": {
                          "sort" : {"$vector" : [0.11, 0.22, 0.33, 0.44]},
                          "filter" : {"status" : "active"},
                          "options" : {"includeSimilarity" : true}
                        }
                      }
                      """;

      final DocumentProjector projector = DocumentProjector.createFromDefinition(null, true);

      FindOneCommand command = objectMapper.readValue(json, FindOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindCollectionOperation.class,
              find -> {
                TextCollectionFilter filter =
                    new TextCollectionFilter("status", MapCollectionFilter.Operator.EQ, "active");

                float[] vector = new float[] {0.11f, 0.22f, 0.33f, 0.44f};
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(projector);
                assertThat(find.pageSize()).isEqualTo(1);
                assertThat(find.limit()).isEqualTo(1);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isTrue();
                assertThat(find.vector()).containsExactly(vector);
                assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                assertThat(find.includeSortVector()).isFalse();
              });
    }

    @Test
    public void filterConditionAndVectorSearchWithIncludeSortVector() throws Exception {
      String json =
          """
          {
            "findOne": {
              "sort" : {"$vector" : [0.11, 0.22, 0.33, 0.44]},
              "filter" : {"status" : "active"},
              "options" : {"includeSortVector" : true}
            }
          }
          """;

      final DocumentProjector projector = DocumentProjector.defaultProjector();

      FindOneCommand command = objectMapper.readValue(json, FindOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindCollectionOperation.class,
              find -> {
                TextCollectionFilter filter =
                    new TextCollectionFilter("status", MapCollectionFilter.Operator.EQ, "active");

                float[] vector = new float[] {0.11f, 0.22f, 0.33f, 0.44f};
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(projector);
                assertThat(find.pageSize()).isEqualTo(1);
                assertThat(find.limit()).isEqualTo(1);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isTrue();
                assertThat(find.vector()).containsExactly(vector);
                assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                assertThat(find.includeSortVector()).isTrue();
              });
    }

    @Test
    public void filterConditionWithIncludeSortVector() throws Exception {
      String json =
          """
              {
                "findOne": {
                  "filter" : {"status" : "active"},
                  "options" : {"includeSortVector" : true}
                }
              }
              """;

      final DocumentProjector projector = DocumentProjector.defaultProjector();

      FindOneCommand command = objectMapper.readValue(json, FindOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindCollectionOperation.class,
              find -> {
                TextCollectionFilter filter =
                    new TextCollectionFilter("status", MapCollectionFilter.Operator.EQ, "active");

                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext()).isEqualTo(commandContext);
                assertThat(find.projection()).isEqualTo(projector);
                assertThat(find.pageSize()).isEqualTo(1);
                assertThat(find.limit()).isEqualTo(1);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isTrue();
                assertThat(find.vector()).isNull();
                assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                assertThat(find.includeSortVector()).isTrue();
              });
    }

    @Test
    public void noFilterCondition() throws Exception {
      String json =
          """
                          {
                            "findOne": {

                            }
                          }
                          """;

      FindOneCommand command = objectMapper.readValue(json, FindOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindCollectionOperation.class,
              op -> {
                assertThat(op.objectMapper()).isEqualTo(objectMapper);
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.limit()).isEqualTo(1);
                assertThat(op.pageSize()).isEqualTo(1);
                assertThat(op.pageState()).isNull();
                assertThat(op.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(op.dbLogicalExpression().filters()).isEmpty();
                assertThat(op.singleResponse()).isTrue();
              });
    }

    @Test
    public void noFilterConditionEmptyOptions() throws Exception {
      String json =
          """
                          {
                            "findOne": {
                                "options": { }
                            }
                          }
                            """;

      FindOneCommand command = objectMapper.readValue(json, FindOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindCollectionOperation.class,
              op -> {
                assertThat(op.objectMapper()).isEqualTo(objectMapper);
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.limit()).isEqualTo(1);
                assertThat(op.pageSize()).isEqualTo(1);
                assertThat(op.pageState()).isNull();
                assertThat(op.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(op.dbLogicalExpression().filters()).isEmpty();
                assertThat(op.singleResponse()).isTrue();
              });
    }

    @Test
    public void dynamicFilterCondition() throws Exception {
      String json =
          """
                          {
                            "findOne": {
                              "filter" : {"col" : "val"}
                            }
                          }
                          """;

      FindOneCommand command = objectMapper.readValue(json, FindOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindCollectionOperation.class,
              op -> {
                TextCollectionFilter filter =
                    new TextCollectionFilter("col", MapCollectionFilter.Operator.EQ, "val");

                assertThat(op.objectMapper()).isEqualTo(objectMapper);
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.limit()).isEqualTo(1);
                assertThat(op.pageSize()).isEqualTo(1);
                assertThat(op.pageState()).isNull();
                assertThat(op.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                assertThat(op.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                assertThat(op.singleResponse()).isTrue();
              });
    }
  }
}
