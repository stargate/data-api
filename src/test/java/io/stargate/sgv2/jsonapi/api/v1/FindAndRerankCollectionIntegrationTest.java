package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.EmbeddingProviderException;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the {@link
 * io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand} running against a
 * collection.
 */
@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
public class FindAndRerankCollectionIntegrationTest extends AbstractCollectionIntegrationTestBase {

  // used to clean up the collection from a previous test, if non-null
  private String cleanupCollectionName = null;

  @BeforeAll
  public final void createDefaultCollection() {
    // override, we do not want the basic collection from the base class.
  }

  @AfterEach
  public void cleanup() {
    if (cleanupCollectionName != null) {
      String toDelete = cleanupCollectionName;
      cleanupCollectionName = null; // clear out just in case
      deleteCollection(toDelete);
    }
  }

  @Test
  public void failOnVectorDisabled() {
    errorOnNotEnabled(
        "vector_not_enabled",
        """
        {
          "name" : "%s",
          "options": {}
        }
        """,
        "UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION",
        "The collection \"%s\".%s does not have vectors enabled.");
  }

  @Test
  void failOnVectorizeDisabled() {
    errorOnNotEnabled(
        "vectorize_not_enabled",
        """
        {
          "name" : "%s",
          "options": {
            "vector": {
                  "metric": "cosine",
                  "dimension": 1024
            }
          }
        }
        """,
        "UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION",
        "The collection \"%s\".%s does not have vectorize enabled.");
  }

  @Test
  void failOnLexicalDisabled() {
    errorOnNotEnabled(
        "lexical_not_enabled",
        """
        {
          "name" : "%s",
          "options": {
            "vector": {
                    "metric": "cosine",
                    "dimension": 1024,
                    "service": {
                        "provider": "openai",
                        "modelName": "text-embedding-3-small"
                    }
                },
            "lexical": {
                "enabled": false
            }
          }
        }
        """,
        "LEXICAL_NOT_ENABLED_FOR_COLLECTION",
        "only be used on collections for which Lexical feature is enabled");
  }

  @Test
  void failOnHybridLimitsAboveMax() {
    // Validation now runs in FindAndRerankOperationBuilder.build(), so the collection must exist.
    Assumptions.assumeTrue(isLexicalAvailableForDB());

    String collectionName = "find_rerank_hybrid_limits_too_large";
    createCollectionWithCleanup(
        collectionName,
        """
            {
              "name" : "%s",
              "options": {
                "vector": {
                        "metric": "cosine",
                        "dimension": 1024,
                        "service": {
                            "provider": "openai",
                            "modelName": "text-embedding-3-small"
                        }
                    },
                "lexical": {
                  "enabled": true,
                  "analyzer": "standard"
                }
              }
            }
            """);

    var rerank =
        """
            {"findAndRerank": {
                    "filter": {},
                    "projection": {},
                    "sort": {
                        "$hybrid": "hybrid sort"
                    },
                    "options": {
                        "limit" : 10,
                        "hybridLimits" : 101,
                        "includeScores": false,
                        "includeSortVector": false
                    }
                }
            }
            """;

    givenHeadersPostJsonThen(keyspaceName, collectionName, rerank)
        .body("$", responseIsError())
        .body("errors[0].errorCode", is(RequestException.Code.COMMAND_FIELD_VALUE_INVALID.name()))
        .body("errors[0].message", containsString("hybridLimits.$vector"))
        .body("errors[0].message", containsString("101"))
        .body("errors[0].message", containsString("must be between 1 and 100"));
  }

  // https://github.com/stargate/data-api/issues/2057
  @Test
  void failOnEmptyRequest() {
    // Must not fail for "no lexical available", so skip on DSE
    Assumptions.assumeTrue(isLexicalAvailableForDB());

    String collectionName = "find_rerank_empty_request";
    createCollectionWithCleanup(
        collectionName,
        """
            {
              "name" : "%s",
              "options": {
                "vector": {
                        "metric": "cosine",
                        "dimension": 1024,
                        "service": {
                            "provider": "openai",
                            "modelName": "text-embedding-3-small"
                        }
                    },
                "lexical": {
                  "enabled": true,
                  "analyzer": "standard"
                }
              }
            }
            """);

    givenHeadersPostJsonThen(keyspaceName, collectionName, "{\"findAndRerank\": { } }")
        .body("$", responseIsError())
        .body("errors[0].errorCode", is(RequestException.Code.MISSING_RERANK_QUERY_TEXT.name()))
        .body(
            "errors[0].message",
            containsString(
                "findAndRerank command is missing the text to use as the query with the reranking"));
  }

  private void errorOnNotEnabled(
      String collectionName, String collectionSpec, String errorCode, String errorMessageContains) {
    createCollectionWithCleanup(collectionName, collectionSpec);

    var rerank =
        """
          {"findAndRerank": {
                  "filter": {},
                  "projection": {},
                  "sort": {
                      "$hybrid": "hybrid sort"
                  },
                  "options": {
                      "limit" : 10,
                      "hybridLimits" : 10,
                      "includeScores": true,
                      "includeSortVector": false
                  }
              }
          }
          """;

    givenHeadersPostJsonThen(keyspaceName, collectionName, rerank)
        .body("$", responseIsError())
        .body("errors[0].errorCode", is(errorCode))
        .body(
            "errors[0].message",
            containsString(errorMessageContains.formatted(keyspaceName, collectionName)));
  }

  // ---- Reranking override tests ----
  // These use a collection with vectorize enabled but NO reranking configured.
  // Sort uses $hybrid with only $vectorize (no $lexical) to avoid needing lexical support.

  private static final String VECTORIZE_NO_RERANK_SPEC =
      """
      {
        "name" : "%s",
        "options": {
          "vector": {
            "metric": "cosine",
            "dimension": 1024,
            "service": {
              "provider": "openai",
              "modelName": "text-embedding-3-small"
            }
          },
          "rerank": {
            "enabled": false
          }
        }
      }
      """;

  @Test
  void failOnRerankingDisabledNoOverride() {
    String collectionName = "rerank_disabled_no_override";
    createCollectionWithCleanup(collectionName, VECTORIZE_NO_RERANK_SPEC);

    givenHeadersPostJsonThen(keyspaceName, collectionName, findAndRerankWithOverride(null))
        .body("$", responseIsError())
        .body("errors[0].errorCode", is(RequestException.Code.UNSUPPORTED_RERANKING_COMMAND.name()))
        .body(
            "errors[0].message",
            containsString("reranking service override was not provided with the command"));
  }

  // Empty override object (`"rerank": {}`) is treated as no override, falling back to the
  // collection's reranking config. On a collection without reranking enabled, this surfaces as
  // UNSUPPORTED_RERANKING_COMMAND — not INVALID_RERANK_OVERRIDE — confirming the empty payload
  // is intentionally ignored rather than validated as a (malformed) override.
  @Test
  void emptyRerankOverrideIgnoredAndFallsThroughToCollectionConfig() {
    String collectionName = "rerank_override_empty";
    createCollectionWithCleanup(collectionName, VECTORIZE_NO_RERANK_SPEC);

    givenHeadersPostJsonThen(keyspaceName, collectionName, findAndRerankWithOverride("{}"))
        .body("$", responseIsError())
        .body("errors[0].errorCode", is(RequestException.Code.UNSUPPORTED_RERANKING_COMMAND.name()))
        .body(
            "errors[0].message",
            containsString("reranking service override was not provided with the command"));
  }

  @Test
  void failOnRerankOverrideUnknownProvider() {
    String collectionName = "rerank_override_bad_provider";
    createCollectionWithCleanup(collectionName, VECTORIZE_NO_RERANK_SPEC);

    givenHeadersPostJsonThen(
            keyspaceName,
            collectionName,
            findAndRerankWithOverride(
                """
                {"provider": "unknown-provider", "modelName": "some-model"}
                """))
        .body("$", responseIsError())
        .body("errors[0].errorCode", is(RequestException.Code.INVALID_RERANK_OVERRIDE.name()))
        .body("errors[0].message", containsString("unknown-provider"));
  }

  @Test
  void failOnRerankOverrideMissingModelName() {
    String collectionName = "rerank_override_no_model";
    createCollectionWithCleanup(collectionName, VECTORIZE_NO_RERANK_SPEC);

    givenHeadersPostJsonThen(
            keyspaceName,
            collectionName,
            findAndRerankWithOverride(
                """
                {"provider": "nvidia"}
                """))
        .body("$", responseIsError())
        .body("errors[0].errorCode", is(RequestException.Code.INVALID_RERANK_OVERRIDE.name()))
        .body("errors[0].message", containsString("Model name is required"));
  }

  @Test
  void failOnRerankOverrideDeprecatedModel() {
    String collectionName = "rerank_override_deprecated";
    createCollectionWithCleanup(collectionName, VECTORIZE_NO_RERANK_SPEC);

    givenHeadersPostJsonThen(
            keyspaceName,
            collectionName,
            findAndRerankWithOverride(
                """
                {"provider": "nvidia", "modelName": "nvidia/a-random-deprecated-model"}
                """))
        .body("$", responseIsError())
        .body("errors[0].errorCode", is(SchemaException.Code.DEPRECATED_AI_MODEL.name()));
  }

  @Test
  void failOnRerankOverrideEolModel() {
    String collectionName = "rerank_override_eol";
    createCollectionWithCleanup(collectionName, VECTORIZE_NO_RERANK_SPEC);

    givenHeadersPostJsonThen(
            keyspaceName,
            collectionName,
            findAndRerankWithOverride(
                """
                {"provider": "nvidia", "modelName": "nvidia/a-random-EOL-model"}
                """))
        .body("$", responseIsError())
        .body("errors[0].errorCode", is(SchemaException.Code.END_OF_LIFE_AI_MODEL.name()));
  }

  @Test
  void failOnRerankOverrideWithUnsupportedAuth() {
    String collectionName = "rerank_override_with_auth";
    createCollectionWithCleanup(collectionName, VECTORIZE_NO_RERANK_SPEC);

    givenHeadersPostJsonThen(
            keyspaceName,
            collectionName,
            findAndRerankWithOverride(
                """
                {"provider": "nvidia", "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2",
                 "authentication": {"providerKey": "my-test-key"}}
                """))
        .body("$", responseIsError())
        .body("errors[0].errorCode", is(RequestException.Code.INVALID_RERANK_OVERRIDE.name()))
        .body("errors[0].message", containsString("authentication"));
  }

  @Test
  void failOnRerankOverrideWithAuthAndParameters() {
    String collectionName = "rerank_override_auth_params";
    createCollectionWithCleanup(collectionName, VECTORIZE_NO_RERANK_SPEC);

    givenHeadersPostJsonThen(
            keyspaceName,
            collectionName,
            findAndRerankWithOverride(
                """
                {"provider": "nvidia", "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2",
                 "authentication": {"providerKey": "my-test-key"},
                 "parameters": {"truncate": "END"}}
                """))
        .body("$", responseIsError())
        .body("errors[0].errorCode", is(RequestException.Code.INVALID_RERANK_OVERRIDE.name()))
        .body("errors[0].message", containsString("authentication"));
  }

  @Test
  void failOnRerankOverrideWithUnsupportedParameters() {
    String collectionName = "rerank_override_params_only";
    createCollectionWithCleanup(collectionName, VECTORIZE_NO_RERANK_SPEC);

    givenHeadersPostJsonThen(
            keyspaceName,
            collectionName,
            findAndRerankWithOverride(
                """
                {"provider": "nvidia", "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2",
                 "parameters": {"truncate": "END"}}
                """))
        .body("$", responseIsError())
        .body("errors[0].errorCode", is(RequestException.Code.INVALID_RERANK_OVERRIDE.name()))
        .body("errors[0].message", containsString("parameters"));
  }

  /**
   * Verifies that a valid rerank override (provider + model only, no auth or params) passes
   * validation and the pipeline proceeds to the embedding (vectorize) step, which fails with
   * EMBEDDING_PROVIDER_CLIENT_ERROR because there is no real OpenAI API key in the test
   * environment.
   */
  @Test
  void overrideWithProviderAndModelPassesValidation() {
    String collectionName = "rerank_override_valid";
    createCollectionWithCleanup(collectionName, VECTORIZE_NO_RERANK_SPEC);

    givenHeadersPostJsonThen(
            keyspaceName,
            collectionName,
            findAndRerankWithOverride(
                """
                {"provider": "nvidia", "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2"}
                """))
        .body("$", responseIsError())
        .body(
            "errors[0].errorCode",
            is(EmbeddingProviderException.Code.EMBEDDING_PROVIDER_CLIENT_ERROR.name()));
  }

  /**
   * Builds a findAndRerank command JSON with an optional rerank override in options.
   *
   * @param rerankOverrideJson JSON object for the "rerank" option, or null for no override.
   */
  private static String findAndRerankWithOverride(String rerankOverrideJson) {
    String rerankOption = rerankOverrideJson != null ? ", \"rerank\": " + rerankOverrideJson : "";
    return
        """
        {"findAndRerank": {
            "sort": {"$hybrid": {"$vectorize": "search text"}},
            "options": {
                "limit": 10%s
            }
        }}
        """
        .formatted(rerankOption);
  }

  private void createCollectionWithCleanup(String collectionName, String collectionSpec) {
    createComplexCollection(collectionSpec.formatted(collectionName));
    // save the collection name for cleanup, but only after successful creation
    cleanupCollectionName = collectionName;
  }
}
