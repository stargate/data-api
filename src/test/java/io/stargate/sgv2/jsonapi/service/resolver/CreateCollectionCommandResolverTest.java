package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CreateCollectionOperation;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(EnabledVectorizeProfile.class)
class CreateCollectionCommandResolverTest {

  @Inject ObjectMapper objectMapper;
  @Inject CreateCollectionCommandResolver resolver;

  @Nested
  class ResolveCommand {

    CommandContext<KeyspaceSchemaObject> commandContext = TestConstants.KEYSPACE_CONTEXT;

    @Test
    public void happyPath() throws Exception {
      String json =
          """
              {
                "createCollection": {
                  "name" : "my_collection"
                }
              }
              """;

      CreateCollectionCommand command = objectMapper.readValue(json, CreateCollectionCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateCollectionOperation.class,
              op -> {
                assertThat(op.name()).isEqualTo("my_collection");
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.vectorSearch()).isEqualTo(false);
                assertThat(op.vectorSize()).isEqualTo(0);
                assertThat(op.vectorFunction()).isNull();
              });
    }

    @Test
    public void happyPathVectorSearch() throws Exception {
      String json =
          """
            {
              "createCollection": {
                "name" : "my_collection",
                "options": {
                  "vector": {
                    "dimension": 4,
                    "metric": "cosine"
                  }
                }
              }
            }
            """;

      CreateCollectionCommand command = objectMapper.readValue(json, CreateCollectionCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateCollectionOperation.class,
              op -> {
                assertThat(op.name()).isEqualTo("my_collection");
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.vectorSearch()).isEqualTo(true);
                assertThat(op.vectorSize()).isEqualTo(4);
                assertThat(op.vectorFunction()).isEqualTo("cosine");
              });
    }

    @Test
    public void happyPathVectorizeSearch() throws Exception {
      String json =
          """
            {
                "createCollection": {
                    "name": "my_collection",
                    "options": {
                        "vector": {
                            "metric": "cosine",
                            "dimension": 768,
                            "service": {
                                "provider": "azureOpenAI",
                                "modelName": "text-embedding-3-small",
                                "parameters": {
                                    "resourceName": "test",
                                    "deploymentId": "test"
                                }
                            }
                        }
                    }
                }
            }
          """;

      CreateCollectionCommand command = objectMapper.readValue(json, CreateCollectionCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateCollectionOperation.class,
              op -> {
                assertThat(op.name()).isEqualTo("my_collection");
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.vectorSearch()).isEqualTo(true);
                assertThat(op.vectorSize()).isEqualTo(768);
                assertThat(op.vectorFunction()).isEqualTo("cosine");
                assertThat(op.comment())
                    .isEqualTo(
                        "{\"collection\":{\"name\":\"my_collection\",\"schema_version\":1,\"options\":{\"vector\":{\"dimension\":768,\"metric\":\"cosine\",\"sourceModel\":\"OTHER\",\"service\":{\"provider\":\"azureOpenAI\",\"modelName\":\"text-embedding-3-small\",\"parameters\":{\"resourceName\":\"test\",\"deploymentId\":\"test\"}}},\"defaultId\":{\"type\":\"\"}}}}",
                        TableCommentConstants.SCHEMA_VERSION_VALUE);
              });
    }

    @Test
    public void happyPathIndexing() throws Exception {
      String json =
          """
          {
            "createCollection": {
              "name" : "my_collection",
              "options": {
                "vector": {
                  "dimension": 4,
                  "metric": "cosine"
                },
                "indexing": {
                  "deny" : ["comment"]
                }
              }
            }
          }
          """;

      CreateCollectionCommand command = objectMapper.readValue(json, CreateCollectionCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateCollectionOperation.class,
              op -> {
                assertThat(op.name()).isEqualTo("my_collection");
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.vectorSearch()).isEqualTo(true);
                assertThat(op.vectorSize()).isEqualTo(4);
                assertThat(op.vectorFunction()).isEqualTo("cosine");
                assertThat(op.comment())
                    .isEqualTo(
                        "{\"collection\":{\"name\":\"my_collection\",\"schema_version\":%s,\"options\":{\"indexing\":{\"deny\":[\"comment\"]},\"vector\":{\"dimension\":4,\"metric\":\"cosine\",\"sourceModel\":\"OTHER\"},\"defaultId\":{\"type\":\"\"}}}}",
                        TableCommentConstants.SCHEMA_VERSION_VALUE);
              });
    }

    @Test
    public void indexingOptionsError() throws Exception {
      String json =
          """
          {
            "createCollection": {
              "name" : "my_collection",
              "options": {
                "vector": {
                  "dimension": 4,
                  "metric": "cosine"
                },
                "indexing": {
                  "deny" : ["comment"],
                  "allow" : ["data"]
                }
              }
            }
          }
          """;

      CreateCollectionCommand command = objectMapper.readValue(json, CreateCollectionCommand.class);
      Throwable throwable = catchThrowable(() -> resolver.resolveCommand(commandContext, command));

      assertThat(throwable)
          .isInstanceOf(JsonApiException.class)
          .satisfies(
              e -> {
                JsonApiException exception = (JsonApiException) e;
                assertThat(exception.getMessage())
                    .isEqualTo(
                        "Invalid indexing definition: `allow` and `deny` cannot be used together");
                assertThat(exception.getErrorCode())
                    .isEqualTo(ErrorCodeV1.INVALID_INDEXING_DEFINITION);
              });
    }

    @Test
    public void happyPathVectorSearchDefaultFunction() throws Exception {
      String json =
          """
        {
          "createCollection": {
            "name" : "my_collection",
            "options": {
              "vector": {
                "dimension": 4
              }
            }
          }
        }
        """;

      CreateCollectionCommand command = objectMapper.readValue(json, CreateCollectionCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateCollectionOperation.class,
              op -> {
                assertThat(op.name()).isEqualTo("my_collection");
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.vectorSearch()).isEqualTo(true);
                assertThat(op.vectorSize()).isEqualTo(4);
                assertThat(op.vectorFunction()).isEqualTo("COSINE");
              });
    }
  }
}
