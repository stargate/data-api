package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;
import static io.stargate.sgv2.jsonapi.util.asserts.DataAPIAsserts.assertThatSchemaException;
import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.util.profiles.EnabledVectorizeProfile;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@QuarkusTest
@TestProfile(EnabledVectorizeProfile.class)
class CreateCollectionCommandResolverTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CreateCollectionCommandResolverTest.class);

  @Inject private CreateCollectionCommandResolver RESOLVER;
  private final TestConstants TEST_CONSTANTS = new TestConstants();

  // want a single instance for all calls, keyspaceContext() creates a new each call
  private final CommandContext<KeyspaceSchemaObject> COMMAND_CONTEXT =
      TEST_CONSTANTS.keyspaceContext();

  private Throwable resolveCommandThrows(String testName, String rawJson) {
    return catchThrowable(
        () -> resolveCommand(testName, rawJson, TEST_CONSTANTS.COLLECTION_IDENTIFIER.table()));
  }

  private CreateCollectionOperation resolveCommand(String testName, String rawJson) {
    return resolveCommand(testName, rawJson, TEST_CONSTANTS.COLLECTION_IDENTIFIER.table());
  }

  private CreateCollectionOperation resolveCommand(
      String testName, String rawJson, CqlIdentifier collectionName) {

    var json = TEST_CONSTANTS.subsRawNames(rawJson);
    LOGGER.info("resolveCommand() - testName: {}, json: {}", testName, json);

    CreateCollectionCommand command;
    try {
      command = TEST_CONSTANTS.OBJECT_MAPPER.readValue(json, CreateCollectionCommand.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    var operation = RESOLVER.resolveCommand(COMMAND_CONTEXT, command);

    assertThat(operation)
        .isInstanceOfSatisfying(
            CreateCollectionOperation.class,
            op -> {
              assertThat(op.collectionName())
                  .as("%s - collectionName() matches command", testName)
                  .isEqualTo(collectionName);
              assertThat(op.commandContext())
                  .as("%s - commandContext() is same object", testName)
                  .isSameAs(COMMAND_CONTEXT);
            });
    return (CreateCollectionOperation) operation;
  }

  @Test
  public void successWithDefaults() {
    var operation =
        resolveCommand(
            "successWithDefaults()",
            """
                        {
                            "createCollection": {
                                "name": "${collection}"
                            }
                        }
                        """);

    assertThat(operation.docIdDesc()).isNull();
    assertThat(operation.indexingDesc()).isNull();
    assertThat(operation.vectorDesc()).isNull();
    assertThat(operation.indexingDesc()).isNull();
    assertThat(operation.lexicalDef())
        .isEqualTo(
            COMMAND_CONTEXT.requestContext().schemaRegistry().lexicalDef().currentVersion(null));
    assertThat(operation.rerankDef())
        .isEqualTo(
            COMMAND_CONTEXT.requestContext().schemaRegistry().rerankDef().currentVersion(null));
  }

  @Test
  public void successWithSupportedNames() {

    String[] supportedNames = {"a", "A", "0", "_", "a0", "0a_A", "_0a"};
    for (String name : supportedNames) {
      var json =
              """
                    {
                        "createCollection": {
                            "name": "%s"
                        }
                    }
                    """
              .formatted(name);
      resolveCommand("successWithSupportedNames()", json, cqlIdentifierFromUserInput(name));
    }
  }

  @Test
  public void successWithVector() {
    var operation =
        resolveCommand(
            "successWithVector()",
            """
                        {
                            "createCollection": {
                                "name": "${collection}",
                                "options": {
                                    "vector": {
                                        "dimension": 4,
                                        "metric": "cosine",
                                        "sourceModel": "openai-v3-large"
                                    }
                                }
                            }
                        }
                        """);

    var expectedVectorDesc =
        new CreateCollectionCommand.Options.VectorSearchDesc(
            4, "cosine", EmbeddingSourceModel.OPENAI_V3_LARGE.apiName(), null);

    assertThat(operation.vectorDesc()).isEqualTo(expectedVectorDesc);
  }

  @Test
  public void successWithVectorDefaultMetric() {

    var operation =
        resolveCommand(
            "successWithVectorDefaultMetric()",
            """
                        {
                            "createCollection": {
                                "name": "${collection}",
                                "options": {
                                    "vector": {
                                        "dimension": 4
                                    }
                                }
                            }
                        }
                        """);

    // aaron 15 june 26 - not sure why but we need the name of the source model must be the CQL name
    // cql cares about capitals, we dont when processing this normally
    var expectedVectorDesc =
        new CreateCollectionCommand.Options.VectorSearchDesc(
            4,
            SimilarityFunction.COSINE.cqlIndexingFunction(),
            EmbeddingSourceModel.OTHER.cqlName(),
            null);

    assertThat(operation.vectorDesc()).isEqualTo(expectedVectorDesc);
  }

  @Test
  public void successWithVectorize() {
    var operation =
        resolveCommand(
            "successWithVector()",
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
                                                "resourceName": "testResourceName",
                                                "deploymentId": "testResourceName"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        """);

    // NOTE: source model of null turns into DEFAULT
    // aaron 15 june 26 - not sure why but we need the name of the source model must be the CQL name
    // cql cares about capitals, we dont when processing this normally
    var expectedVectorDesc =
        new CreateCollectionCommand.Options.VectorSearchDesc(
            768,
            "cosine",
            EmbeddingSourceModel.DEFAULT.cqlName(),
            new VectorizeConfig(
                "azureOpenAI",
                "text-embedding-3-small",
                null,
                Map.of("resourceName", "testResourceName", "deploymentId", "testResourceName")));

    assertThat(operation.vectorDesc()).isEqualTo(expectedVectorDesc);
  }

  @Test
  public void successWithDenyIndexing() {
    var operation =
        resolveCommand(
            "successWithDenyIndexing()",
            """
                        {
                            "createCollection": {
                                "name": "${collection}",
                                "options": {
                                    "indexing": {
                                        "deny": [
                                            "comment"
                                        ]
                                    }
                                }
                            }
                        }
                        """);

    var expectedIndexing =
        new CreateCollectionCommand.Options.IndexingDesc(null, List.of("comment"));

    assertThat(operation.indexingDesc()).isEqualTo(expectedIndexing);
  }

  @Test
  public void failWithUndefinedName() {

    var throwable =
        resolveCommandThrows(
            "failWithUndefinedName()",
            """
                        {
                            "createCollection": {}
                        }
                        """);

    assertThatSchemaException(throwable)
        .as("failWithUndefinedName()")
        .hasCode(SchemaException.Code.UNSUPPORTED_SCHEMA_NAME)
        .hasMessageSnippets(
            "The command attempted to create a Collection with a name that is not supported.",
            "The supported Collection names must not be empty, more than 48 characters long, or contain non-alphanumeric-underscore characters.",
            "The command used the unsupported Collection name: '(null)'.");
  }

  @Test
  public void failWithEmptyName() {

    var throwable =
        resolveCommandThrows(
            "failWithUndefinedName()",
            """
                        {
                            "createCollection": {
                                "name": ""
                            }
                        }
                        """);

    assertThatSchemaException(throwable)
        .as("failWithEmptyName()")
        .hasCode(SchemaException.Code.UNSUPPORTED_SCHEMA_NAME)
        .hasMessageSnippets(
            "The command attempted to create a Collection with a name that is not supported.",
            "The supported Collection names must not be empty, more than 48 characters long, or contain non-alphanumeric-underscore characters.",
            "The command used the unsupported Collection name: ''.");
  }

  @Test
  public void failWithBlankName() {

    // Blank is a white space
    var throwable =
        resolveCommandThrows(
            "failWithBlankName()",
            """
                        {
                            "createCollection": {
                                "name": "  "
                            }
                        }
                        """);

    assertThatSchemaException(throwable)
        .as("failWithBlankName()")
        .hasCode(SchemaException.Code.UNSUPPORTED_SCHEMA_NAME)
        .hasMessageSnippets(
            "The command attempted to create a Collection with a name that is not supported.",
            "The supported Collection names must not be empty, more than 48 characters long, or contain non-alphanumeric-underscore characters.",
            "The command used the unsupported Collection name: '  '.");
  }

  @Test
  public void failWithNameTooLong() {

    var longName = RandomStringUtils.insecure().nextAlphabetic(49);
    var throwable =
        resolveCommandThrows(
            "failWithNameTooLong()",
                """
                        {
                            "createCollection": {
                                "name": "%s"
                            }
                        }
                        """
                .formatted(longName));

    assertThatSchemaException(throwable)
        .as("failWithNameTooLong()")
        .hasCode(SchemaException.Code.UNSUPPORTED_SCHEMA_NAME)
        .hasMessageSnippets(
            "The command attempted to create a Collection with a name that is not supported.",
            "The supported Collection names must not be empty, more than 48 characters long, or contain non-alphanumeric-underscore characters.",
            "The command used the unsupported Collection name: '%s'.".formatted(longName));
  }

  @Test
  public void failWithNameSpecialCharacter() {

    var throwable =
        resolveCommandThrows(
            "failWithNameSpecialCharacter()",
            """
                        {
                            "createCollection": {
                                "name": "!@-"
                            }
                        }
                        """);

    assertThatSchemaException(throwable)
        .as("failWithNameSpecialCharacter()")
        .hasCode(SchemaException.Code.UNSUPPORTED_SCHEMA_NAME)
        .hasMessageSnippets(
            "The command attempted to create a Collection with a name that is not supported.",
            "The supported Collection names must not be empty, more than 48 characters long, or contain non-alphanumeric-underscore characters.",
            "The command used the unsupported Collection name: '!@-'.");
  }
}
