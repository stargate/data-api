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
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CreateCollectionOperation;
import jakarta.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(EnabledVectorizeProfile.class)
class CreateCollectionCommandResolverTest {

  @Inject ObjectMapper objectMapper;
  @Inject CreateCollectionCommandResolver resolver;

  private final TestConstants testConstants = new TestConstants();

  CommandContext<KeyspaceSchemaObject> commandContext;

  @BeforeEach
  public void beforeEach() {
    commandContext = testConstants.keyspaceContext();
  }

  @Nested
  class CreateCollectionSuccess {

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
                        "{\"collection\":{\"name\":\"my_collection\",\"schema_version\":1,\"options\":{"
                            + "\"vector\":{\"dimension\":768,\"metric\":\"cosine\",\"sourceModel\":\"OTHER\","
                            + "\"service\":{\"provider\":\"azureOpenAI\",\"modelName\":\"text-embedding-3-small\","
                            + "\"parameters\":{\"resourceName\":\"test\",\"deploymentId\":\"test\"}}},\"defaultId\":{\"type\":\"\"},"
                            + "\"lexical\":{\"enabled\":true,\"analyzer\":\"standard\"},"
                            + "\"rerank\":{\"enabled\":false}}}"
                            + "}",
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
                        "{\"collection\":{\"name\":\"my_collection\",\"schema_version\":%s,\"options\":{\"indexing\":{\"deny\":[\"comment\"]},"
                            + "\"vector\":{\"dimension\":4,\"metric\":\"cosine\",\"sourceModel\":\"OTHER\"},\"defaultId\":{\"type\":\"\"},"
                            + "\"lexical\":{\"enabled\":true,\"analyzer\":\"standard\"},"
                            + "\"rerank\":{\"enabled\":false}}}"
                            + "}",
                        TableCommentConstants.SCHEMA_VERSION_VALUE);
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

    @Test
    public void createCollectionWithSupportedName() throws Exception {
      String[] supportedName = {"a", "A", "0", "_", "a0", "0a_A", "_0a"};
      for (String name : supportedName) {
        String json =
                """
              {
                "createCollection": {
                  "name" : "%s"
                }
              }
              """
                .formatted(name);

        CreateCollectionCommand command =
            objectMapper.readValue(json, CreateCollectionCommand.class);
        Operation result = resolver.resolveCommand(commandContext, command);

        assertThat(result)
            .isInstanceOfSatisfying(
                CreateCollectionOperation.class,
                op -> {
                  assertThat(op.name()).isEqualTo(name);
                  assertThat(op.commandContext()).isEqualTo(commandContext);
                  assertThat(op.vectorSearch()).isEqualTo(false);
                  assertThat(op.vectorSize()).isEqualTo(0);
                  assertThat(op.vectorFunction()).isNull();
                });
      }
    }
  }

  @Nested
  class CreateCollectionFailure {

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
    public void createCollectionWithNull() throws Exception {
      String json =
          """
          {
            "createCollection": {
            }
          }
          """;

      CreateCollectionCommand command = objectMapper.readValue(json, CreateCollectionCommand.class);
      Throwable throwable = catchThrowable(() -> resolver.resolveCommand(commandContext, command));

      verifySchemaException(
          throwable,
          SchemaException.Code.UNSUPPORTED_SCHEMA_NAME,
          "The command attempted to create a Collection with a name that is not supported.",
          "The supported Collection names must not be empty, more than 48 characters long, or contain non-alphanumeric-underscore characters.",
          "The command used the unsupported Collection name: '(null)'.");
    }

    @Test
    public void createCollectionWithEmptyName() throws Exception {
      String json =
          """
          {
            "createCollection": {
              "name" : ""
            }
          }
          """;

      CreateCollectionCommand command = objectMapper.readValue(json, CreateCollectionCommand.class);
      Throwable throwable = catchThrowable(() -> resolver.resolveCommand(commandContext, command));

      verifySchemaException(
          throwable,
          SchemaException.Code.UNSUPPORTED_SCHEMA_NAME,
          "The command attempted to create a Collection with a name that is not supported.",
          "The supported Collection names must not be empty, more than 48 characters long, or contain non-alphanumeric-underscore characters.",
          "The command used the unsupported Collection name: ''.");
    }

    @Test
    public void createCollectionWithBlankName() throws Exception {
      String json =
          """
          {
            "createCollection": {
              "name": " "
            }
          }
          """;

      CreateCollectionCommand command = objectMapper.readValue(json, CreateCollectionCommand.class);
      Throwable throwable = catchThrowable(() -> resolver.resolveCommand(commandContext, command));

      verifySchemaException(
          throwable,
          SchemaException.Code.UNSUPPORTED_SCHEMA_NAME,
          "The command attempted to create a Collection with a name that is not supported.",
          "The supported Collection names must not be empty, more than 48 characters long, or contain non-alphanumeric-underscore characters.",
          "The command used the unsupported Collection name: ' '.");
    }

    @Test
    public void createCollectionWithNameTooLong() throws Exception {
      String name = RandomStringUtils.insecure().nextAlphabetic(49);
      String json =
              """
          {
            "createCollection": {
              "name": "%s"
            }
          }
          """
              .formatted(name);

      CreateCollectionCommand command = objectMapper.readValue(json, CreateCollectionCommand.class);
      Throwable throwable = catchThrowable(() -> resolver.resolveCommand(commandContext, command));

      verifySchemaException(
          throwable,
          SchemaException.Code.UNSUPPORTED_SCHEMA_NAME,
          "The command attempted to create a Collection with a name that is not supported.",
          "The supported Collection names must not be empty, more than 48 characters long, or contain non-alphanumeric-underscore characters.",
          "The command used the unsupported Collection name: '%s'.".formatted(name));
    }

    @Test
    public void createCollectionWithSpecialCharacter() throws Exception {
      String json =
          """
          {
            "createCollection": {
              "name": "!@-"
            }
          }
          """;

      CreateCollectionCommand command = objectMapper.readValue(json, CreateCollectionCommand.class);
      Throwable throwable = catchThrowable(() -> resolver.resolveCommand(commandContext, command));

      verifySchemaException(
          throwable,
          SchemaException.Code.UNSUPPORTED_SCHEMA_NAME,
          "The command attempted to create a Collection with a name that is not supported.",
          "The supported Collection names must not be empty, more than 48 characters long, or contain non-alphanumeric-underscore characters.",
          "The command used the unsupported Collection name: '!@-'.");
    }
  }

  private void verifySchemaException(
      Throwable throwable, SchemaException.Code exceptedErrorCode, String... messageSnippet) {
    assertThat(throwable)
        .isInstanceOf(SchemaException.class)
        .satisfies(
            e -> {
              SchemaException exception = (SchemaException) e;
              assertThat(exception.code).isEqualTo(exceptedErrorCode.name());
              for (String snippet : messageSnippet) {
                assertThat(exception.getMessage()).contains(snippet);
              }
            });
  }
}
