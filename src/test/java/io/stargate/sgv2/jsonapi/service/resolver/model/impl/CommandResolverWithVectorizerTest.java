package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndDeleteCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndReplaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndUpdateCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.UpdateOneCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.embedding.DataVectorizer;
import io.stargate.sgv2.jsonapi.service.embedding.DataVectorizerService;
import io.stargate.sgv2.jsonapi.service.embedding.operation.TestEmbeddingService;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DeleteOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.InsertOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.ReadAndUpdateOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import io.stargate.sgv2.jsonapi.service.testutil.DocumentUpdaterUtils;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import jakarta.inject.Inject;
import java.util.Optional;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class CommandResolverWithVectorizerTest {
  @Inject ObjectMapper objectMapper;
  @Inject OperationsConfig operationsConfig;
  @Inject Shredder shredder;

  @InjectMock protected DataApiRequestInfo dataApiRequestInfo;

  @Inject FindCommandResolver findCommandResolver;

  @Inject FindOneCommandResolver findOneCommandResolver;
  @Inject UpdateOneCommandResolver updateOneCommandResolver;
  @Inject DeleteOneCommandResolver deleteOneCommandResolver;

  @Inject FindOneAndDeleteCommandResolver findOneAndDeleteCommandResolver;

  @Inject FindOneAndReplaceCommandResolver findOneAndReplaceCommandResolver;

  @Inject FindOneAndUpdateCommandResolver findOneAndUpdateCommandResolver;

  @Inject InsertManyCommandResolver insertManyCommandResolver;

  @Inject InsertOneCommandResolver insertOneCommandResolver;

  @Inject DataVectorizerService dataVectorizerService;

  @Nested
  class Resolve {
    protected final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
    protected final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);
    private final CommandContext VECTOR_COMMAND_CONTEXT =
        new CommandContext(
            KEYSPACE_NAME,
            COLLECTION_NAME,
            new CollectionSettings(
                COLLECTION_NAME,
                true,
                -1,
                CollectionSettings.SimilarityFunction.COSINE,
                null,
                null,
                null),
            null);

    @Test
    public void find() throws Exception {
      String json =
          """
          {
            "find": {
              "sort" : {"$vectorize" : "test data"}
            }
          }
          """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);
      final FindCommand vectorizedCommand =
          (FindCommand)
              dataVectorizerService
                  .vectorize(TestEmbeddingService.commandContextWithVectorize, findOneCommand)
                  .subscribe()
                  .withSubscriber(UniAssertSubscriber.create())
                  .awaitItem()
                  .getItem();
      Operation operation =
          findCommandResolver.resolveCommand(
              TestEmbeddingService.commandContextWithVectorize, vectorizedCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                float[] vector = new float[] {0.25f, 0.25f, 0.25f};
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext())
                    .isEqualTo(TestEmbeddingService.commandContextWithVectorize);
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
    public void findNonVectorize() throws Exception {
      String json =
          """
          {
            "find": {
              "sort" : {"$vectorize" : "test data"}
            }
          }
          """;

      FindCommand findOneCommand = objectMapper.readValue(json, FindCommand.class);

      Throwable throwable =
          dataVectorizerService
              .vectorize(VECTOR_COMMAND_CONTEXT, findOneCommand)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .getFailure();

      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              e -> {
                JsonApiException exception = (JsonApiException) e;
                assertThat(exception.getMessage())
                    .isEqualTo(
                        "Unable to vectorize data, embedding service not configured for the collection");
                assertThat(exception.getErrorCode())
                    .isEqualTo(ErrorCode.UNAVAILABLE_EMBEDDING_SERVICE);
              });
    }

    @Test
    public void deleteOne() throws Exception {
      String json =
          """
          {
            "deleteOne": {
              "filter" : {"col" : "val"},
              "sort" : {"$vectorize" : "test data"}
            }
          }
          """;

      DeleteOneCommand deleteOneCommand = objectMapper.readValue(json, DeleteOneCommand.class);
      final DeleteOneCommand vectorizedCommand =
          (DeleteOneCommand)
              dataVectorizerService
                  .vectorize(TestEmbeddingService.commandContextWithVectorize, deleteOneCommand)
                  .subscribe()
                  .withSubscriber(UniAssertSubscriber.create())
                  .awaitItem()
                  .getItem();
      Operation operation =
          deleteOneCommandResolver.resolveCommand(
              TestEmbeddingService.commandContextWithVectorize, vectorizedCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              DeleteOperation.class,
              op -> {
                assertThat(op.commandContext())
                    .isEqualTo(TestEmbeddingService.commandContextWithVectorize);
                assertThat(op.deleteLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          DBFilterBase.TextFilter filter =
                              new DBFilterBase.TextFilter(
                                  "col", DBFilterBase.MapFilterBase.Operator.EQ, "val");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext())
                              .isEqualTo(TestEmbeddingService.commandContextWithVectorize);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.KEY);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                          assertThat(find.orderBy()).isNull();
                          assertThat(find.vector()).isNotNull();
                          assertThat(find.vector()).containsExactly(0.25f, 0.25f, 0.25f);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void updateOne() throws Exception {
      String json =
          """
            {
              "updateOne": {
                "filter" : {"col" : "val"},
                "update" : {"$set" : {"location" : "New York"}},
                "sort" : {"$vectorize" : "test data"}
              }
            }
            """;

      UpdateOneCommand command = objectMapper.readValue(json, UpdateOneCommand.class);
      final UpdateOneCommand vectorizedCommand =
          (UpdateOneCommand)
              dataVectorizerService
                  .vectorize(TestEmbeddingService.commandContextWithVectorize, command)
                  .subscribe()
                  .withSubscriber(UniAssertSubscriber.create())
                  .awaitItem()
                  .getItem();
      Operation operation =
          updateOneCommandResolver.resolveCommand(
              TestEmbeddingService.commandContextWithVectorize, vectorizedCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateOperation.class,
              op -> {
                assertThat(op.commandContext())
                    .isEqualTo(TestEmbeddingService.commandContextWithVectorize);
                assertThat(op.returnDocumentInResponse()).isFalse();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.shredder()).isEqualTo(shredder);
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
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          DBFilterBase.TextFilter filter =
                              new DBFilterBase.TextFilter(
                                  "col", DBFilterBase.MapFilterBase.Operator.EQ, "val");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext())
                              .isEqualTo(TestEmbeddingService.commandContextWithVectorize);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                          assertThat(find.vector()).isNotNull();
                          assertThat(find.vector()).containsExactly(0.25f, 0.25f, 0.25f);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void updateNonVectorize() throws Exception {
      String json =
          """
                    {
                      "updateOne": {
                        "filter" : {"col" : "val"},
                        "update" : {"$set" : {"location" : "New York"}},
                        "sort" : {"$vectorize" : "test data"}
                      }
                    }
                    """;

      UpdateOneCommand command = objectMapper.readValue(json, UpdateOneCommand.class);
      Throwable throwable =
          dataVectorizerService
              .vectorize(VECTOR_COMMAND_CONTEXT, command)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .getFailure();
      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              e -> {
                JsonApiException exception = (JsonApiException) e;
                assertThat(exception.getMessage())
                    .isEqualTo(
                        "Unable to vectorize data, embedding service not configured for the collection");
                assertThat(exception.getErrorCode())
                    .isEqualTo(ErrorCode.UNAVAILABLE_EMBEDDING_SERVICE);
              });
    }

    @Test
    public void findOneAndDelete() throws Exception {
      String json =
          """
        {
          "findOneAndDelete": {
            "filter" : {"status" : "active"},
            "sort" : {"$vectorize" : "test data"}
          }
        }
        """;

      FindOneAndDeleteCommand command = objectMapper.readValue(json, FindOneAndDeleteCommand.class);
      final FindOneAndDeleteCommand vectorizedCommand =
          (FindOneAndDeleteCommand)
              dataVectorizerService
                  .vectorize(TestEmbeddingService.commandContextWithVectorize, command)
                  .subscribe()
                  .withSubscriber(UniAssertSubscriber.create())
                  .awaitItem()
                  .getItem();
      Operation operation =
          findOneAndDeleteCommandResolver.resolveCommand(
              TestEmbeddingService.commandContextWithVectorize, vectorizedCommand);
      assertThat(operation)
          .isInstanceOfSatisfying(
              DeleteOperation.class,
              op -> {
                assertThat(op.commandContext())
                    .isEqualTo(TestEmbeddingService.commandContextWithVectorize);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          DBFilterBase.TextFilter filter =
                              new DBFilterBase.TextFilter(
                                  "status", DBFilterBase.MapFilterBase.Operator.EQ, "active");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext())
                              .isEqualTo(TestEmbeddingService.commandContextWithVectorize);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                          assertThat(find.vector()).isNotNull();
                          assertThat(find.vector()).containsExactly(0.25f, 0.25f, 0.25f);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void findOneAndReplace() throws Exception {
      String json =
          """
        {
          "findOneAndReplace": {
            "filter" : {"status" : "active"},
            "sort" : {"$vectorize" : "test data"},
            "replacement" : {"col1" : "val1", "col2" : "val2", "$vectorize" : "test data"}
          }
        }
        """;

      FindOneAndReplaceCommand command =
          objectMapper.readValue(json, FindOneAndReplaceCommand.class);
      final FindOneAndReplaceCommand vectorizedCommand =
          (FindOneAndReplaceCommand)
              dataVectorizerService
                  .vectorize(TestEmbeddingService.commandContextWithVectorize, command)
                  .subscribe()
                  .withSubscriber(UniAssertSubscriber.create())
                  .awaitItem()
                  .getItem();
      Operation operation =
          findOneAndReplaceCommandResolver.resolveCommand(
              TestEmbeddingService.commandContextWithVectorize, vectorizedCommand);
      String expected =
          "{\"col1\":\"val1\",\"col2\":\"val2\",\"$vectorize\":\"test data\",\"$vector\":[0.25,0.25,0.25]}";
      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateOperation.class,
              op -> {
                assertThat(op.commandContext())
                    .isEqualTo(TestEmbeddingService.commandContextWithVectorize);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.shredder()).isEqualTo(shredder);
                assertThat(op.updateLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.documentUpdater())
                    .isInstanceOfSatisfying(
                        DocumentUpdater.class,
                        replacer -> {
                          try {
                            ObjectNode replacement =
                                (ObjectNode)
                                    objectMapper.readTree(
                                        "{\"col1\" : \"val1\", \"col2\" : \"val2\"}");
                          } catch (JsonProcessingException e) {
                            e.printStackTrace();
                          }
                          assertThat(replacer.replaceDocument().toString()).isEqualTo(expected);
                          assertThat(replacer.replaceDocumentId()).isNull();
                        });
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          DBFilterBase.TextFilter filter =
                              new DBFilterBase.TextFilter(
                                  "status", DBFilterBase.MapFilterBase.Operator.EQ, "active");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext())
                              .isEqualTo(TestEmbeddingService.commandContextWithVectorize);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                          assertThat(find.vector()).isNotNull();
                          assertThat(find.vector()).containsExactly(0.25f, 0.25f, 0.25f);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void findOneAndUpdate() throws Exception {
      String json =
          """
            {
              "findOneAndUpdate": {
                "filter" : {"status" : "active"},
                "sort" : {"$vector" : [0.11, 0.22, 0.33, 0.44]},
                "update" : {"$set" : {"$vectorize" : "test data"}}
              }
            }
            """;

      FindOneAndUpdateCommand command = objectMapper.readValue(json, FindOneAndUpdateCommand.class);
      final FindOneAndUpdateCommand vectorizedCommand =
          (FindOneAndUpdateCommand)
              dataVectorizerService
                  .vectorize(TestEmbeddingService.commandContextWithVectorize, command)
                  .subscribe()
                  .withSubscriber(UniAssertSubscriber.create())
                  .awaitItem()
                  .getItem();
      Operation operation =
          findOneAndUpdateCommandResolver.resolveCommand(
              TestEmbeddingService.commandContextWithVectorize, vectorizedCommand);
      UpdateClause updateClause =
          DocumentUpdaterUtils.updateClause(
              UpdateOperator.SET, objectMapper.createObjectNode().put("$vectorize", "test data"));

      new DataVectorizer(
              TestEmbeddingService.commandContextWithVectorize.embeddingService(),
              objectMapper.getNodeFactory(),
              Optional.empty())
          .vectorizeUpdateClause(updateClause)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .getItem();
      ;
      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateOperation.class,
              op -> {
                assertThat(op.commandContext())
                    .isEqualTo(TestEmbeddingService.commandContextWithVectorize);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.shredder()).isEqualTo(shredder);
                assertThat(op.updateLimit()).isEqualTo(1);
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
                          DBFilterBase.TextFilter filter =
                              new DBFilterBase.TextFilter(
                                  "status", DBFilterBase.MapFilterBase.Operator.EQ, "active");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext())
                              .isEqualTo(TestEmbeddingService.commandContextWithVectorize);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                          assertThat(find.vector()).isNotNull();
                          assertThat(find.vector()).containsExactly(0.11f, 0.22f, 0.33f, 0.44f);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void findOne() throws Exception {
      String json =
          """
          {
            "findOne": {
              "sort" : {"$vectorize" : "test data"},
              "filter" : {"status" : "active"}
            }
          }
          """;

      FindOneCommand command = objectMapper.readValue(json, FindOneCommand.class);
      final FindOneCommand vectorizedCommand =
          (FindOneCommand)
              dataVectorizerService
                  .vectorize(TestEmbeddingService.commandContextWithVectorize, command)
                  .subscribe()
                  .withSubscriber(UniAssertSubscriber.create())
                  .awaitItem()
                  .getItem();
      Operation operation =
          findOneCommandResolver.resolveCommand(
              TestEmbeddingService.commandContextWithVectorize, vectorizedCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindOperation.class,
              find -> {
                DBFilterBase.TextFilter filter =
                    new DBFilterBase.TextFilter(
                        "status", DBFilterBase.MapFilterBase.Operator.EQ, "active");

                float[] vector = new float[] {0.25f, 0.25f, 0.25f};
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext())
                    .isEqualTo(TestEmbeddingService.commandContextWithVectorize);
                assertThat(find.projection()).isEqualTo(DocumentProjector.identityProjector());
                assertThat(find.pageSize()).isEqualTo(1);
                assertThat(find.limit()).isEqualTo(1);
                assertThat(find.pageState()).isNull();
                assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                assertThat(find.skip()).isZero();
                assertThat(find.maxSortReadLimit()).isZero();
                assertThat(find.singleResponse()).isTrue();
                assertThat(find.vector()).containsExactly(vector);
                assertThat(
                        find.logicalExpression().comparisonExpressions.get(0).getDbFilters().get(0))
                    .isEqualTo(filter);
              });
    }

    @Test
    public void insertMany() throws Exception {
      String json =
          """
          {
            "insertMany": {
              "documents": [
                {
                  "_id": "1",
                  "location": "London",
                  "$vectorize" : "test data"
                },
                {
                  "_id": "2",
                  "location": "New York",
                  "$vectorize" : "test data"
                }
              ]
            }
          }
          """;

      InsertManyCommand command = objectMapper.readValue(json, InsertManyCommand.class);
      final InsertManyCommand vectorizedCommand =
          (InsertManyCommand)
              dataVectorizerService
                  .vectorize(TestEmbeddingService.commandContextWithVectorize, command)
                  .subscribe()
                  .withSubscriber(UniAssertSubscriber.create())
                  .awaitItem()
                  .getItem();
      Operation result =
          insertManyCommandResolver.resolveCommand(
              TestEmbeddingService.commandContextWithVectorize, vectorizedCommand);
      assertThat(result)
          .isInstanceOfSatisfying(
              InsertOperation.class,
              op -> {
                WritableShreddedDocument first = shredder.shred(command.documents().get(0));
                WritableShreddedDocument second = shredder.shred(command.documents().get(1));
                assertThat(first.queryVectorValues().length).isEqualTo(3);
                assertThat(first.queryVectorValues()).containsExactly(0.25f, 0.25f, 0.25f);
                assertThat(second.queryVectorValues().length).isEqualTo(3);
                assertThat(second.queryVectorValues()).containsExactly(0.25f, 0.25f, 0.25f);
                assertThat(op.commandContext())
                    .isEqualTo(TestEmbeddingService.commandContextWithVectorize);
                assertThat(op.ordered()).isFalse();
                assertThat(op.documents()).containsExactly(first, second);
              });
    }

    @Test
    public void insertManyNonVectorize() throws Exception {
      String json =
          """
                  {
                    "insertMany": {
                      "documents": [
                        {
                          "_id": "1",
                          "location": "London",
                          "$vectorize" : "test data"
                        },
                        {
                          "_id": "2",
                          "location": "New York",
                          "$vectorize" : "test data"
                        }
                      ]
                    }
                  }
                  """;

      InsertManyCommand command = objectMapper.readValue(json, InsertManyCommand.class);
      final Throwable throwable =
          dataVectorizerService
              .vectorize(VECTOR_COMMAND_CONTEXT, command)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .getFailure();

      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              e -> {
                JsonApiException exception = (JsonApiException) e;
                assertThat(exception.getMessage())
                    .isEqualTo(
                        "Unable to vectorize data, embedding service not configured for the collection");
                assertThat(exception.getErrorCode())
                    .isEqualTo(ErrorCode.UNAVAILABLE_EMBEDDING_SERVICE);
              });
    }

    @Test
    public void insertOne() throws Exception {
      String json =
          """
        {
          "insertOne": {
            "document" : {
              "_id": "1",
              "$vectorize" : "test data"
            }
          }
        }
        """;

      InsertOneCommand command = objectMapper.readValue(json, InsertOneCommand.class);
      final InsertOneCommand vectorizedCommand =
          (InsertOneCommand)
              dataVectorizerService
                  .vectorize(TestEmbeddingService.commandContextWithVectorize, command)
                  .subscribe()
                  .withSubscriber(UniAssertSubscriber.create())
                  .awaitItem()
                  .getItem();
      Operation result =
          insertOneCommandResolver.resolveCommand(
              TestEmbeddingService.commandContextWithVectorize, vectorizedCommand);

      assertThat(result)
          .isInstanceOfSatisfying(
              InsertOperation.class,
              op -> {
                WritableShreddedDocument expected = shredder.shred(command.document());
                assertThat(expected.queryVectorValues().length).isEqualTo(3);
                assertThat(expected.queryVectorValues()).containsExactly(0.25f, 0.25f, 0.25f);
                assertThat(op.commandContext())
                    .isEqualTo(TestEmbeddingService.commandContextWithVectorize);
                assertThat(op.ordered()).isFalse();
                assertThat(op.documents()).singleElement().isEqualTo(expected);
              });
    }
  }
}
