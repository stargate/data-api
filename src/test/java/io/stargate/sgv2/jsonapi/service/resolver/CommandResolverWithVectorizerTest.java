package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndDeleteCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.UpdateOneCommand;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObjectName;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.embedding.DataVectorizerService;
import io.stargate.sgv2.jsonapi.service.embedding.operation.TestEmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.*;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.MapCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.TextCollectionFilter;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.IdConfig;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.service.shredding.collections.WritableShreddedDocument;
import io.stargate.sgv2.jsonapi.service.testutil.DocumentUpdaterUtils;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class CommandResolverWithVectorizerTest {
  @Inject ObjectMapper objectMapper;
  @Inject OperationsConfig operationsConfig;
  @Inject DocumentShredder documentShredder;

  @InjectMock protected RequestContext dataApiRequestInfo;

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
    // TODO: do these need to be unique to this test ? Can we use TestConstants ?
    protected final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
    protected final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);
    private final CommandContext<CollectionSchemaObject> VECTOR_COMMAND_CONTEXT =
        TestConstants.collectionContext(
            "testCommand",
            new CollectionSchemaObject(
                new SchemaObjectName(KEYSPACE_NAME, COLLECTION_NAME),
                null,
                IdConfig.defaultIdConfig(),
                VectorConfig.fromColumnDefinitions(
                    List.of(
                        new VectorColumnDefinition(
                            DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD,
                            -1,
                            SimilarityFunction.COSINE,
                            EmbeddingSourceModel.OTHER,
                            null))),
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
                  .vectorize(
                      TestEmbeddingProvider.commandContextWithVectorize,
                      findOneCommand)
                  .subscribe()
                  .withSubscriber(UniAssertSubscriber.create())
                  .awaitItem()
                  .getItem();
      Operation operation =
          findCommandResolver.resolveCommand(
              TestEmbeddingProvider.commandContextWithVectorize, vectorizedCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindCollectionOperation.class,
              find -> {
                float[] vector = new float[] {0.25f, 0.25f, 0.25f};
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext())
                    .isEqualTo(TestEmbeddingProvider.commandContextWithVectorize);
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
                        "Unable to vectorize data, embedding service not configured for the collection : "
                            + VECTOR_COMMAND_CONTEXT.schemaObject().name().table());
                assertThat(exception.getErrorCode())
                    .isEqualTo(ErrorCodeV1.EMBEDDING_SERVICE_NOT_CONFIGURED);
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
                  .vectorize(
                      TestEmbeddingProvider.commandContextWithVectorize,
                      deleteOneCommand)
                  .subscribe()
                  .withSubscriber(UniAssertSubscriber.create())
                  .awaitItem()
                  .getItem();
      Operation operation =
          deleteOneCommandResolver.resolveCommand(
              TestEmbeddingProvider.commandContextWithVectorize, vectorizedCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              DeleteCollectionOperation.class,
              op -> {
                assertThat(op.commandContext())
                    .isEqualTo(TestEmbeddingProvider.commandContextWithVectorize);
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
                          assertThat(find.commandContext())
                              .isEqualTo(TestEmbeddingProvider.commandContextWithVectorize);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.KEY);
                          assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
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
                  .vectorize(
                      TestEmbeddingProvider.commandContextWithVectorize,
                      command)
                  .subscribe()
                  .withSubscriber(UniAssertSubscriber.create())
                  .awaitItem()
                  .getItem();
      Operation operation =
          updateOneCommandResolver.resolveCommand(
              TestEmbeddingProvider.commandContextWithVectorize, vectorizedCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateCollectionOperation.class,
              op -> {
                assertThat(op.commandContext())
                    .isEqualTo(TestEmbeddingProvider.commandContextWithVectorize);
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
                          assertThat(find.commandContext())
                              .isEqualTo(TestEmbeddingProvider.commandContextWithVectorize);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                          assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
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
              .vectorize( VECTOR_COMMAND_CONTEXT, command)
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
                        "Unable to vectorize data, embedding service not configured for the collection : "
                            + VECTOR_COMMAND_CONTEXT.schemaObject().name().table());
                assertThat(exception.getErrorCode())
                    .isEqualTo(ErrorCodeV1.EMBEDDING_SERVICE_NOT_CONFIGURED);
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
                  .vectorize(
                      TestEmbeddingProvider.commandContextWithVectorize,
                      command)
                  .subscribe()
                  .withSubscriber(UniAssertSubscriber.create())
                  .awaitItem()
                  .getItem();
      Operation operation =
          findOneAndDeleteCommandResolver.resolveCommand(
              TestEmbeddingProvider.commandContextWithVectorize, vectorizedCommand);
      assertThat(operation)
          .isInstanceOfSatisfying(
              DeleteCollectionOperation.class,
              op -> {
                assertThat(op.commandContext())
                    .isEqualTo(TestEmbeddingProvider.commandContextWithVectorize);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.findCollectionOperation())
                    .isInstanceOfSatisfying(
                        FindCollectionOperation.class,
                        find -> {
                          TextCollectionFilter filter =
                              new TextCollectionFilter(
                                  "status", MapCollectionFilter.Operator.EQ, "active");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext())
                              .isEqualTo(TestEmbeddingProvider.commandContextWithVectorize);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                          assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                          assertThat(find.vector()).isNotNull();
                          assertThat(find.vector()).containsExactly(0.25f, 0.25f, 0.25f);
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
                  .vectorize(
                      TestEmbeddingProvider.commandContextWithVectorize,
                      command)
                  .subscribe()
                  .withSubscriber(UniAssertSubscriber.create())
                  .awaitItem()
                  .getItem();
      Operation operation =
          findOneCommandResolver.resolveCommand(
              TestEmbeddingProvider.commandContextWithVectorize, vectorizedCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindCollectionOperation.class,
              find -> {
                TextCollectionFilter filter =
                    new TextCollectionFilter("status", MapCollectionFilter.Operator.EQ, "active");

                float[] vector = new float[] {0.25f, 0.25f, 0.25f};
                assertThat(find.objectMapper()).isEqualTo(objectMapper);
                assertThat(find.commandContext())
                    .isEqualTo(TestEmbeddingProvider.commandContextWithVectorize);
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
                  "$vectorize" : "return 1s"
                }
              ]
            }
          }
          """;

      InsertManyCommand command = objectMapper.readValue(json, InsertManyCommand.class);
      final InsertManyCommand vectorizedCommand =
          (InsertManyCommand)
              dataVectorizerService
                  .vectorize(
                      TestEmbeddingProvider.commandContextWithVectorize,
                      command)
                  .subscribe()
                  .withSubscriber(UniAssertSubscriber.create())
                  .awaitItem()
                  .getItem();
      Operation result =
          insertManyCommandResolver.resolveCommand(
              TestEmbeddingProvider.commandContextWithVectorize, vectorizedCommand);
      assertThat(result)
          .isInstanceOfSatisfying(
              InsertCollectionOperation.class,
              op -> {
                WritableShreddedDocument first = documentShredder.shred(command.documents().get(0));
                WritableShreddedDocument second =
                    documentShredder.shred(command.documents().get(1));
                assertThat(first.queryVectorValues().length).isEqualTo(3);
                assertThat(first.queryVectorValues()).containsExactly(0.25f, 0.25f, 0.25f);
                assertThat(second.queryVectorValues().length).isEqualTo(3);
                assertThat(second.queryVectorValues()).containsExactly(1.0f, 1.0f, 1.0f);
                assertThat(op.commandContext())
                    .isEqualTo(TestEmbeddingProvider.commandContextWithVectorize);
                assertThat(op.ordered()).isFalse();
                assertThat(op.insertions()).hasSize(2);
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
              .vectorize( VECTOR_COMMAND_CONTEXT, command)
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
                        "Unable to vectorize data, embedding service not configured for the collection : "
                            + VECTOR_COMMAND_CONTEXT.schemaObject().name().table());
                assertThat(exception.getErrorCode())
                    .isEqualTo(ErrorCodeV1.EMBEDDING_SERVICE_NOT_CONFIGURED);
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
                  .vectorize(

                      TestEmbeddingProvider.commandContextWithVectorize,
                      command)
                  .subscribe()
                  .withSubscriber(UniAssertSubscriber.create())
                  .awaitItem()
                  .getItem();
      Operation result =
          insertOneCommandResolver.resolveCommand(
              TestEmbeddingProvider.commandContextWithVectorize, vectorizedCommand);

      assertThat(result)
          .isInstanceOfSatisfying(
              InsertCollectionOperation.class,
              op -> {
                WritableShreddedDocument expected = documentShredder.shred(command.document());
                assertThat(expected.queryVectorValues().length).isEqualTo(3);
                assertThat(expected.queryVectorValues()).containsExactly(0.25f, 0.25f, 0.25f);
                assertThat(op.commandContext())
                    .isEqualTo(TestEmbeddingProvider.commandContextWithVectorize);
                assertThat(op.ordered()).isFalse();
                assertThat(op.insertions()).hasSize(1);
              });
    }
  }
}
