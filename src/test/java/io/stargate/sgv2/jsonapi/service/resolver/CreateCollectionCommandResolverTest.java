package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;
import static io.stargate.sgv2.jsonapi.util.asserts.DataAPIAsserts.assertThatSchemaException;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import java.util.List;

import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests how the {@link CreateCollectionCommandResolver} handles inputs and the operation it
 * creates.
 *
 * <p><b>NOTE:</b> subclassed atleast by {@link CreateCollectionCommandResolverVectorizeDisabledTest} to
 * change the vectorize enabled setting
 */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class CreateCollectionCommandResolverTest extends CreateCollectionCommandResolverTestBase {

  protected static final Logger LOGGER =
      LoggerFactory.getLogger(CreateCollectionCommandResolverTest.class);

  @Test
  public void successWithDefaults() {
    var operation =
        assertResolver(
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
      assertResolver("successWithSupportedNames()", json, cqlIdentifierFromUserInput(name));
    }
  }

  @Test
  public void successWithVector() {
    var operation =
        assertResolver(
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
        assertResolver(
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
  public void successWithDenyIndexing() {
    var operation =
        assertResolver(
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
        assertResolverThrows(
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
        assertResolverThrows(
            "failWithEmptyName()",
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
        assertResolverThrows(
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
        assertResolverThrows(
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
        assertResolverThrows(
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
