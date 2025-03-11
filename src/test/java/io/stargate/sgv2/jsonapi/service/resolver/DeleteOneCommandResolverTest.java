package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteOneCommand;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.collections.DeleteCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.IDCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.MapCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.TextCollectionFilter;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class DeleteOneCommandResolverTest {
  @Inject ObjectMapper objectMapper;
  @Inject OperationsConfig operationsConfig;
  @Inject DeleteOneCommandResolver resolver;
  @InjectMock protected RequestContext dataApiRequestInfo;
  private TestConstants testConstants = new TestConstants();

  CommandContext<CollectionSchemaObject> commandContext;

  @BeforeEach
  public void beforeEach() {
    commandContext = testConstants.collectionContext();
  }

  @Nested
  class DeleteOneCommandResolveCommand {

    @Test
    public void idFilterCondition() throws Exception {
      String json =
          """
          {
            "deleteOne": {
              "filter" : {"_id" : "id"}
            }
          }
          """;

      DeleteOneCommand deleteOneCommand = objectMapper.readValue(json, DeleteOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, deleteOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              DeleteCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.deleteLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.findCollectionOperation())
                    .isInstanceOfSatisfying(
                        FindCollectionOperation.class,
                        find -> {
                          IDCollectionFilter filter =
                              new IDCollectionFilter(
                                  IDCollectionFilter.Operator.EQ, DocumentId.fromString("id"));

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.KEY);
                          assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void noFilterCondition() throws Exception {
      String json =
          """
          {
            "deleteOne": {
            }
          }
          """;

      DeleteOneCommand deleteOneCommand = objectMapper.readValue(json, DeleteOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, deleteOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              DeleteCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.deleteLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.findCollectionOperation())
                    .isInstanceOfSatisfying(
                        FindCollectionOperation.class,
                        find -> {
                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.KEY);
                          assertThat(find.dbLogicalExpression().filters()).isEmpty();
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void dynamicFilterCondition() throws Exception {
      String json =
          """
          {
            "deleteOne": {
              "filter" : {"col" : "val"}
            }
          }
          """;

      DeleteOneCommand deleteOneCommand = objectMapper.readValue(json, DeleteOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, deleteOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              DeleteCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.deleteLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.findCollectionOperation())
                    .isInstanceOfSatisfying(
                        FindCollectionOperation.class,
                        find -> {
                          TextCollectionFilter filter =
                              new TextCollectionFilter(
                                  "col", MapCollectionFilter.Operator.EQ, "val");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.KEY);
                          assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void dynamicFilterConditionWithSort() throws Exception {
      String json =
          """
                  {
                    "deleteOne": {
                      "filter" : {"col" : "val"},
                      "sort" : {"sort_col" : 1}
                    }
                  }
                  """;

      DeleteOneCommand deleteOneCommand = objectMapper.readValue(json, DeleteOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, deleteOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              DeleteCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.deleteLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.findCollectionOperation())
                    .isInstanceOfSatisfying(
                        FindCollectionOperation.class,
                        find -> {
                          TextCollectionFilter filter =
                              new TextCollectionFilter(
                                  "col", MapCollectionFilter.Operator.EQ, "val");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(100);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.SORTED_DOCUMENT);
                          assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                          assertThat(find.orderBy()).hasSize(1);
                          assertThat(find.orderBy().get(0))
                              .isEqualTo(new FindCollectionOperation.OrderBy("sort_col", true));
                          assertThat(find.maxSortReadLimit())
                              .isEqualTo(operationsConfig.maxDocumentSortCount());
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void dynamicFilterConditionWithVectorSearch() throws Exception {
      String json =
          """
              {
                "deleteOne": {
                  "filter" : {"col" : "val"},
                  "sort" : {"$vector" : [0.11, 0.22, 0.33, 0.44]}
                }
              }
              """;

      DeleteOneCommand deleteOneCommand = objectMapper.readValue(json, DeleteOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, deleteOneCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              DeleteCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.deleteLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.findCollectionOperation())
                    .isInstanceOfSatisfying(
                        FindCollectionOperation.class,
                        find -> {
                          TextCollectionFilter filter =
                              new TextCollectionFilter(
                                  "col", MapCollectionFilter.Operator.EQ, "val");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.KEY);
                          assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                          assertThat(find.orderBy()).isNull();
                          assertThat(find.vector()).isNotNull();
                          assertThat(find.vector()).containsExactly(0.11f, 0.22f, 0.33f, 0.44f);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }
  }
}
