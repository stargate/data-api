package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.operation.collections.CreateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(EnabledVectorizeProfile.class)
class CreateCollectionCommandResolverTest {

  @Inject ObjectMapper objectMapper;
  @Inject CreateCollectionCommandResolver resolver;

  private final TestConstants TEST_CONSTANTS = new TestConstants();

  CommandContext<KeyspaceSchemaObject> commandContext;

  @BeforeEach
  public void beforeEach() {
    commandContext = TEST_CONSTANTS.keyspaceContext();
  }

  @Nested
  class CreateCollectionSuccess {

    @Test
    public void happyPath() throws Exception {
      var json =
          TEST_CONSTANTS.subsRawNames(
              """
              {
                "createCollection": {
                  "name" : "${collection}"
                }
              }
              """);

      var command = objectMapper.readValue(json, CreateCollectionCommand.class);
      var operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              CreateCollectionOperation.class,
              op -> {
                assertThat(op.collectionName())
                    .isEqualTo(TEST_CONSTANTS.COLLECTION_IDENTIFIER.table());
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.vectorDesc()).isNull();
              });
    }

    @Test
    public void happyPathVectorSearch() throws Exception {
      var json =
          TEST_CONSTANTS.subsRawNames(
              """
          {
            "createCollection": {
              "name" : "${collection}",
              "options": {
                "vector": {
                  "dimension": 4,
                  "metric": "cosine"
                }
              }
            }
          }
          """);

      var command = objectMapper.readValue(json, CreateCollectionCommand.class);
      var operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              CreateCollectionOperation.class,
              op -> {
                assertThat(op.collectionName())
                    .isEqualTo(TEST_CONSTANTS.COLLECTION_IDENTIFIER.table());
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.vectorDesc()).isNotNull();
                assertThat(op.vectorDesc().dimension()).isEqualTo(4);
                assertThat(op.vectorDesc().metric()).isEqualTo("cosine");
              });
    }

    @Test
    public void happyPathVectorSearchWithIndexOptions() throws Exception {
      var json =
          TEST_CONSTANTS.subsRawNames(
              """
          {
            "createCollection": {
              "name" : "${collection}",
              "options": {
                "vector": {
                  "dimension": 4,
                  "metric": "cosine",
                  "indexOptions": {
                    "maximumNodeConnections": 32,
                    "constructionBeamWidth": 200,
                    "enableHierarchy": true
                  }
                }
              }
            }
          }
          """);

      var command = objectMapper.readValue(json, CreateCollectionCommand.class);
      var operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              CreateCollectionOperation.class,
              op -> {
                assertThat(op.vectorDesc()).isNotNull();
                assertThat(op.vectorDesc().indexOptions()).isNotNull();
                assertThat(op.vectorDesc().indexOptions().maximumNodeConnections()).isEqualTo(32);
                assertThat(op.vectorDesc().indexOptions().constructionBeamWidth()).isEqualTo(200);
                assertThat(op.vectorDesc().indexOptions().enableHierarchy()).isTrue();
              });
    }

    @Test
    public void happyPathVectorizeSearch() throws Exception {
      var json =
          TEST_CONSTANTS.subsRawNames(
              """
            {
                "createCollection": {
                    "name": "${collection}",
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
          """);
      // NOTE: source model of null turns into DEFAULT
      var expectedVectorDesc =
          new CreateCollectionCommand.Options.VectorSearchDesc(
              768,
              "cosine",
              EmbeddingSourceModel.DEFAULT.cqlName(),
              new VectorizeConfig(
                  "azureOpenAI",
                  "text-embedding-3-small",
                  null,
                  Map.of("resourceName", "test", "deploymentId", "test")),
              null);

      var command = objectMapper.readValue(json, CreateCollectionCommand.class);
      var operation = resolver.resolveCommand(commandContext, command);

      // NOTE: this used to check the table comment string that was created, that has moved to the
      // CreateCollectionOperationTest
      assertThat(operation)
          .isInstanceOfSatisfying(
              CreateCollectionOperation.class,
              op -> {
                assertThat(op.collectionName())
                    .isEqualTo(TEST_CONSTANTS.COLLECTION_IDENTIFIER.table());
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.vectorDesc()).isEqualTo(expectedVectorDesc);
              });
    }

    @Test
    public void happyPathIndexing() throws Exception {

      var json =
          TEST_CONSTANTS.subsRawNames(
              """
          {
            "createCollection": {
              "name" : "${collection}",
              "options": {
                "indexing": {
                  "deny" : ["comment"]
                }
              }
            }
          }
          """);
      var expectedIndexing =
          new CreateCollectionCommand.Options.IndexingDesc(null, List.of("comment"));

      var command = objectMapper.readValue(json, CreateCollectionCommand.class);
      var operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              CreateCollectionOperation.class,
              op -> {
                assertThat(op.collectionName())
                    .isEqualTo(TEST_CONSTANTS.COLLECTION_IDENTIFIER.table());
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.indexingDesc()).isEqualTo(expectedIndexing);
              });
    }

    @Test
    public void happyPathVectorSearchDefaultFunction() throws Exception {

      var json =
          TEST_CONSTANTS.subsRawNames(
              """
          {
            "createCollection": {
              "name" : "${collection}",
              "options": {
                "vector": {
                  "dimension": 4
                }
              }
            }
          }
          """);

      var command = objectMapper.readValue(json, CreateCollectionCommand.class);
      var operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              CreateCollectionOperation.class,
              op -> {
                assertThat(op.collectionName())
                    .isEqualTo(TEST_CONSTANTS.COLLECTION_IDENTIFIER.table());
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.vectorDesc()).isNotNull();
                assertThat(op.vectorDesc().dimension()).isEqualTo(4);
                assertThat(op.vectorDesc().metric()).isEqualTo("COSINE");
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

        var command = objectMapper.readValue(json, CreateCollectionCommand.class);
        var operation = resolver.resolveCommand(commandContext, command);

        assertThat(operation)
            .isInstanceOfSatisfying(
                CreateCollectionOperation.class,
                op -> {
                  assertThat(op.collectionName()).isEqualTo(cqlIdentifierFromUserInput(name));
                  assertThat(op.commandContext()).isEqualTo(commandContext);
                  assertThat(op.vectorDesc()).isNull();
                });
      }
    }
  }

  @Nested
  class CreateCollectionFailure {

    @Test
    public void indexingOptionsError() throws Exception {

      var json =
          TEST_CONSTANTS.subsRawNames(
              """
                  {
                    "createCollection": {
                      "name" : "${collection}",
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
                  """);

      var command = objectMapper.readValue(json, CreateCollectionCommand.class);
      var throwable = catchThrowable(() -> resolver.resolveCommand(commandContext, command));

      assertThat(throwable)
          .isInstanceOf(SchemaException.class)
          .satisfies(
              e -> {
                SchemaException exception = (SchemaException) e;
                assertThat(exception.getMessage())
                    .containsSequence(
                        "'createCollection' indexing definition invalid: 'allow' and 'deny' cannot be used together");
                assertThat(exception.code)
                    .isEqualTo(SchemaException.Code.INVALID_INDEXING_DEFINITION.name());
              });
    }

    @Test
    public void createCollectionWithNull() throws Exception {

      var json =
          """
          {
            "createCollection": {
            }
          }
          """;

      var command = objectMapper.readValue(json, CreateCollectionCommand.class);
      var throwable = catchThrowable(() -> resolver.resolveCommand(commandContext, command));

      verifySchemaException(
          throwable,
          SchemaException.Code.UNSUPPORTED_SCHEMA_NAME,
          "The command attempted to create a Collection with a name that is not supported.",
          "The supported Collection names must not be empty, more than 48 characters long, or contain non-alphanumeric-underscore characters.",
          "The command used the unsupported Collection name: '(null)'.");
    }

    @Test
    public void createCollectionWithEmptyName() throws Exception {

      var json =
          """
          {
            "createCollection": {
              "name" : ""
            }
          }
          """;

      var command = objectMapper.readValue(json, CreateCollectionCommand.class);
      var throwable = catchThrowable(() -> resolver.resolveCommand(commandContext, command));

      verifySchemaException(
          throwable,
          SchemaException.Code.UNSUPPORTED_SCHEMA_NAME,
          "The command attempted to create a Collection with a name that is not supported.",
          "The supported Collection names must not be empty, more than 48 characters long, or contain non-alphanumeric-underscore characters.",
          "The command used the unsupported Collection name: ''.");
    }

    @Test
    public void createCollectionWithBlankName() throws Exception {

      var json =
          """
          {
            "createCollection": {
              "name": " "
            }
          }
          """;

      var command = objectMapper.readValue(json, CreateCollectionCommand.class);
      var throwable = catchThrowable(() -> resolver.resolveCommand(commandContext, command));

      verifySchemaException(
          throwable,
          SchemaException.Code.UNSUPPORTED_SCHEMA_NAME,
          "The command attempted to create a Collection with a name that is not supported.",
          "The supported Collection names must not be empty, more than 48 characters long, or contain non-alphanumeric-underscore characters.",
          "The command used the unsupported Collection name: ' '.");
    }

    @Test
    public void createCollectionWithNameTooLong() throws Exception {

      var name = RandomStringUtils.insecure().nextAlphabetic(49);
      var json =
              """
          {
            "createCollection": {
              "name": "%s"
            }
          }
          """
              .formatted(name);

      var command = objectMapper.readValue(json, CreateCollectionCommand.class);
      var throwable = catchThrowable(() -> resolver.resolveCommand(commandContext, command));

      verifySchemaException(
          throwable,
          SchemaException.Code.UNSUPPORTED_SCHEMA_NAME,
          "The command attempted to create a Collection with a name that is not supported.",
          "The supported Collection names must not be empty, more than 48 characters long, or contain non-alphanumeric-underscore characters.",
          "The command used the unsupported Collection name: '%s'.".formatted(name));
    }

    @Test
    public void createCollectionWithSpecialCharacter() throws Exception {

      var json =
          """
          {
            "createCollection": {
              "name": "!@-"
            }
          }
          """;

      var command = objectMapper.readValue(json, CreateCollectionCommand.class);
      var throwable = catchThrowable(() -> resolver.resolveCommand(commandContext, command));

      verifySchemaException(
          throwable,
          SchemaException.Code.UNSUPPORTED_SCHEMA_NAME,
          "The command attempted to create a Collection with a name that is not supported.",
          "The supported Collection names must not be empty, more than 48 characters long, or contain non-alphanumeric-underscore characters.",
          "The command used the unsupported Collection name: '!@-'.");
    }
  }

  private void verifySchemaException(
      Throwable throwable, SchemaException.Code expectedErrorCode, String... messageSnippet) {

    assertThat(throwable)
        .isInstanceOf(SchemaException.class)
        .satisfies(
            e -> {
              SchemaException exception = (SchemaException) e;
              assertThat(exception.code).isEqualTo(expectedErrorCode.name());
              for (String snippet : messageSnippet) {
                assertThat(exception.getMessage()).contains(snippet);
              }
            });
  }
}
