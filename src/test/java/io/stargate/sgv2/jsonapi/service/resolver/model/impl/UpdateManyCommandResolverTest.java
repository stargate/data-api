package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.api.model.command.impl.UpdateManyCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.embedding.DataVectorizer;
import io.stargate.sgv2.jsonapi.service.embedding.operation.TestEmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.ReadAndUpdateOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.collection.IDFilter;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.collection.MapFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.collection.TextFilter;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.testutil.DocumentUpdaterUtils;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.Optional;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class UpdateManyCommandResolverTest {

  @Inject ObjectMapper objectMapper;
  @Inject OperationsConfig operationsConfig;
  @Inject Shredder shredder;
  @Inject UpdateManyCommandResolver resolver;
  @InjectMock protected DataApiRequestInfo dataApiRequestInfo;

  @Nested
  class Resolve {

    CommandContext commandContext = CommandContext.empty();

    @Test
    public void idFilterCondition() throws Exception {
      String json =
          """
          {
            "updateMany": {
              "filter" : {"_id" : "id"},
              "update" : {"$set" : {"location" : "New York"}}
            }
          }
          """;

      UpdateManyCommand command = objectMapper.readValue(json, UpdateManyCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isFalse();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.shredder()).isEqualTo(shredder);
                assertThat(op.updateLimit()).isEqualTo(operationsConfig.maxDocumentUpdateCount());
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
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          IDFilter filter =
                              new IDFilter(
                                  IDFilter.Operator.EQ, DocumentId.fromString("id"));

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                          assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                        });
              });
    }

    @Test
    public void noFilterCondition() throws Exception {
      String json =
          """
          {
            "updateMany": {
              "update" : {"$set" : {"location" : "New York"}}
            }
          }
          """;

      UpdateManyCommand command = objectMapper.readValue(json, UpdateManyCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isFalse();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.shredder()).isEqualTo(shredder);
                assertThat(op.updateLimit()).isEqualTo(operationsConfig.maxDocumentUpdateCount());
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
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                          assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                          assertThat(find.logicalExpression().comparisonExpressions).isEmpty();
                        });
              });
    }

    @Test
    public void dynamicFilterCondition() throws Exception {
      String json =
          """
          {
            "updateMany": {
              "filter" : {"col" : "val"},
                "update" : {"$set" : {"location" : "New York"}}
              }
            }
          """;

      UpdateManyCommand command = objectMapper.readValue(json, UpdateManyCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isFalse();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.shredder()).isEqualTo(shredder);
                assertThat(op.updateLimit()).isEqualTo(operationsConfig.maxDocumentUpdateCount());
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
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          TextFilter filter =
                              new TextFilter(
                                  "col", MapFilterBase.Operator.EQ, "val");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                          assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                        });
              });
    }

    @Test
    public void dynamicFilterConditionSetVectorize() throws Exception {
      String json =
          """
                  {
                    "updateMany": {
                      "filter" : {"col" : "val"},
                      "update" : {"$set" : {"$vectorize" : "test data"}}
                    }
                  }
                  """;

      UpdateManyCommand command = objectMapper.readValue(json, UpdateManyCommand.class);
      Operation operation =
          resolver.resolveCommand(TestEmbeddingProvider.commandContextWithVectorize, command);

      UpdateClause updateClause =
          DocumentUpdaterUtils.updateClause(
              UpdateOperator.SET, objectMapper.createObjectNode().put("$vectorize", "test data"));
      new DataVectorizer(
              TestEmbeddingProvider.commandContextWithVectorize.embeddingProvider(),
              objectMapper.getNodeFactory(),
              Optional.empty(),
              TestEmbeddingProvider.commandContextWithVectorize.collectionSettings())
          .vectorizeUpdateClause(updateClause);
      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateOperation.class,
              op -> {
                assertThat(op.commandContext())
                    .isEqualTo(TestEmbeddingProvider.commandContextWithVectorize);
                assertThat(op.returnDocumentInResponse()).isFalse();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.shredder()).isEqualTo(shredder);
                assertThat(op.updateLimit()).isEqualTo(20);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.documentUpdater())
                    .isInstanceOfSatisfying(
                        DocumentUpdater.class,
                        updater -> {
                          assertThat(updater.updateOperations())
                              .isEqualTo(updateClause.buildOperations());
                        });
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          TextFilter filter =
                              new TextFilter(
                                  "col", MapFilterBase.Operator.EQ, "val");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext())
                              .isEqualTo(TestEmbeddingProvider.commandContextWithVectorize);
                          assertThat(find.pageSize()).isEqualTo(20);
                          assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                          assertThat(find.singleResponse()).isFalse();
                        });
              });
    }

    @Test
    public void withUpsert() throws Exception {
      String json =
          """
          {
            "updateMany": {
              "update" : {"$set" : {"location" : "New York"}},
              "options": { "upsert": true }
            }
          }
          """;

      UpdateManyCommand command = objectMapper.readValue(json, UpdateManyCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isFalse();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isTrue();
                assertThat(op.shredder()).isEqualTo(shredder);
                assertThat(op.updateLimit()).isEqualTo(operationsConfig.maxDocumentUpdateCount());
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
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(operationsConfig.defaultPageSize());
                          assertThat(find.limit()).isEqualTo(Integer.MAX_VALUE);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                          assertThat(find.logicalExpression().comparisonExpressions).isEmpty();
                        });
              });
    }
  }
}
