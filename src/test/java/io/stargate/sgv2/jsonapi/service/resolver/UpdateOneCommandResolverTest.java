package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.api.model.command.impl.UpdateOneCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.collections.ReadAndUpdateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.IDCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.MapCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.TextCollectionFilter;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.service.testutil.DocumentUpdaterUtils;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class UpdateOneCommandResolverTest {

  @Inject ObjectMapper objectMapper;
  @Inject OperationsConfig operationsConfig;
  @Inject DocumentShredder documentShredder;
  @Inject UpdateOneCommandResolver resolver;
  @InjectMock protected DataApiRequestInfo dataApiRequestInfo;

  @Nested
  class Resolve {

    CommandContext<CollectionSchemaObject> commandContext = TestConstants.COLLECTION_CONTEXT;

    @Test
    public void idFilterCondition() throws Exception {
      String json =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "id"},
              "update" : {"$set" : {"location" : "New York"}}
            }
          }
          """;

      UpdateOneCommand command = objectMapper.readValue(json, UpdateOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isFalse();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.documentShredder()).isEqualTo(documentShredder);
                assertThat(op.updateLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.documentUpdater())
                    .isInstanceOfSatisfying(
                        DocumentUpdater.class,
                        updater -> {
                          UpdateClause updateClause =
                              DocumentUpdaterUtils.updateClause(
                                  UpdateOperator.SET,
                                  objectMapper.createObjectNode().put("location", "New York"));

                          assertThat(updater.updateOperations())
                              .isEqualTo(updateClause.buildOperations());
                        });
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
                          assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                          assertThat(find.dbLogicalExpression().dBFilters().get(0))
                              .isEqualTo(filter);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void noFilterCondition() throws Exception {
      String json =
          """
          {
            "updateOne": {
              "update" : {"$set" : {"location" : "New York"}}
            }
          }
          """;

      UpdateOneCommand command = objectMapper.readValue(json, UpdateOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isFalse();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.documentShredder()).isEqualTo(documentShredder);
                assertThat(op.updateLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.documentUpdater())
                    .isInstanceOfSatisfying(
                        DocumentUpdater.class,
                        updater -> {
                          UpdateClause updateClause =
                              DocumentUpdaterUtils.updateClause(
                                  UpdateOperator.SET,
                                  objectMapper.createObjectNode().put("location", "New York"));

                          assertThat(updater.updateOperations())
                              .isEqualTo(updateClause.buildOperations());
                        });
                assertThat(op.findCollectionOperation())
                    .isInstanceOfSatisfying(
                        FindCollectionOperation.class,
                        find -> {
                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                          assertThat(find.dbLogicalExpression().dBFilters()).isEmpty();
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void dynamicFilterCondition() throws Exception {
      String json =
          """
          {
            "updateOne": {
              "filter" : {"col" : "val"},
                "update" : {"$set" : {"location" : "New York"}}
              }
            }
          """;

      UpdateOneCommand command = objectMapper.readValue(json, UpdateOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isFalse();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.documentShredder()).isEqualTo(documentShredder);
                assertThat(op.updateLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.documentUpdater())
                    .isInstanceOfSatisfying(
                        DocumentUpdater.class,
                        updater -> {
                          UpdateClause updateClause =
                              DocumentUpdaterUtils.updateClause(
                                  UpdateOperator.SET,
                                  objectMapper.createObjectNode().put("location", "New York"));

                          assertThat(updater.updateOperations())
                              .isEqualTo(updateClause.buildOperations());
                        });
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
                          assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                          assertThat(find.dbLogicalExpression().dBFilters().get(0))
                              .isEqualTo(filter);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void dynamicFilterConditionWithSort() throws Exception {
      String json =
          """
                  {
                    "updateOne": {
                      "filter" : {"col" : "val"},
                        "update" : {"$set" : {"location" : "New York"}},
                        "sort" : {"sort_col" : 1}
                      }
                    }
                  """;

      UpdateOneCommand command = objectMapper.readValue(json, UpdateOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isFalse();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.documentShredder()).isEqualTo(documentShredder);
                assertThat(op.updateLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.documentUpdater())
                    .isInstanceOfSatisfying(
                        DocumentUpdater.class,
                        updater -> {
                          UpdateClause updateClause =
                              DocumentUpdaterUtils.updateClause(
                                  UpdateOperator.SET,
                                  objectMapper.createObjectNode().put("location", "New York"));

                          assertThat(updater.updateOperations())
                              .isEqualTo(updateClause.buildOperations());
                        });
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
                          assertThat(find.dbLogicalExpression().dBFilters().get(0))
                              .isEqualTo(filter);
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
            "updateOne": {
              "filter" : {"col" : "val"},
              "update" : {"$set" : {"location" : "New York"}},
              "sort" : {"$vector" : [0.11, 0.22, 0.33, 0.44]}
            }
          }
          """;

      UpdateOneCommand command = objectMapper.readValue(json, UpdateOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isFalse();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.documentShredder()).isEqualTo(documentShredder);
                assertThat(op.updateLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.documentUpdater())
                    .isInstanceOfSatisfying(
                        DocumentUpdater.class,
                        updater -> {
                          UpdateClause updateClause =
                              DocumentUpdaterUtils.updateClause(
                                  UpdateOperator.SET,
                                  objectMapper.createObjectNode().put("location", "New York"));

                          assertThat(updater.updateOperations())
                              .isEqualTo(updateClause.buildOperations());
                        });
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
                          assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                          assertThat(find.dbLogicalExpression().dBFilters().get(0))
                              .isEqualTo(filter);
                          assertThat(find.vector()).isNotNull();
                          assertThat(find.vector()).containsExactly(0.11f, 0.22f, 0.33f, 0.44f);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void withUpsert() throws Exception {
      String json =
          """
          {
            "updateOne": {
              "update" : {"$set" : {"location" : "New York"}},
              "options": { "upsert": true }
            }
          }
          """;

      UpdateOneCommand command = objectMapper.readValue(json, UpdateOneCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isFalse();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isTrue();
                assertThat(op.documentShredder()).isEqualTo(documentShredder);
                assertThat(op.updateLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.documentUpdater())
                    .isInstanceOfSatisfying(
                        DocumentUpdater.class,
                        updater -> {
                          UpdateClause updateClause =
                              DocumentUpdaterUtils.updateClause(
                                  UpdateOperator.SET,
                                  objectMapper.createObjectNode().put("location", "New York"));

                          assertThat(updater.updateOperations())
                              .isEqualTo(updateClause.buildOperations());
                        });
                assertThat(op.findCollectionOperation())
                    .isInstanceOfSatisfying(
                        FindCollectionOperation.class,
                        find -> {
                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                          assertThat(find.dbLogicalExpression().dBFilters()).isEmpty();
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }
  }
}
