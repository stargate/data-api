package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.response.ValidatableResponse;
import io.stargate.sgv2.jsonapi.exception.EmbeddingProviderException;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.SortException;
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
        "The collection %s.%s does not have vectors enabled.");
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
        "The collection %s.%s does not have vectorize enabled.");
  }

  @Test
  void successOnLexicalDisabledHybrid() {
    // lexical disabled, and using $hybrid
    // will get EmbeddingProviderException.Code.EMBEDDING_PROVIDER_CLIENT_ERROR.name() because
    // using open AI with no token
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
        EmbeddingProviderException.Code.EMBEDDING_PROVIDER_CLIENT_ERROR.name(),
        "Incorrect API key provided:");
  }

  @Test
  void failOnLexicalDisabled() {
    // lexical disabled, and using $lexical
    var rerank =
        """
                    {"findAndRerank": {
                            "filter": {},
                            "projection": {},
                            "sort": {
                                "$hybrid": {
                                  "$vectorize" : "hello",
                                  "$lexical" : "hello"
                                }
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
        SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION.name(),
        "The collection without a lexical index: %s.%s.",
        rerank);
  }

  @Test
  void failOnHybridLimitsAboveMax() {
    // Validation runs when the command is resolved to an operation, so the collection must exist.
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

  /** Every kind of sort can run on it: vectorize, lexical and reranking are all enabled. */
  private static final String VECTORIZE_LEXICAL_AND_RERANK_SPEC =
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
          },
          "rerank": {
            "enabled": true
          }
        }
      }
      """;

  // https://github.com/stargate/data-api/issues/2577
  @Test
  void failOnSortWithNoVectorOrVectorize() {
    // the $lexical sort needs a lexical index, so skip on DSE
    Assumptions.assumeTrue(isLexicalAvailableForDB());

    // one collection for every sort, every kind of sort can run on it so only the sort is wrong
    String collectionName = "find_rerank_no_vector_sort";
    createCollectionWithCleanup(collectionName, VECTORIZE_LEXICAL_AND_RERANK_SPEC);

    // $lexical with no $vectorize or $vector
    findAndRerankWithSort(
            collectionName,
            """
            {"$hybrid": {"$lexical": "hello"}}
            """)
        .body("$", responseIsError())
        .body(
            "errors[0].errorCode",
            is(SortException.Code.MISSING_VECTOR_OR_VECTORIZE_IN_HYBRID_SORT.name()));

    // $hybrid text is empty
    findAndRerankWithSort(
            collectionName,
            """
            {"$hybrid": ""}
            """)
        .body("$", responseIsError())
        .body("errors[0].errorCode", is(SortException.Code.MISSING_HYBRID_SORT_VALUE.name()));

    // $hybrid object is empty
    findAndRerankWithSort(
            collectionName,
            """
            {"$hybrid": {}}
            """)
        .body("$", responseIsError())
        .body("errors[0].errorCode", is(SortException.Code.MISSING_HYBRID_SORT_VALUE.name()));

    // no sort
    findAndRerankWithSort(collectionName, null)
        .body("$", responseIsError())
        .body(
            "errors[0].errorCode", is(SortException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT.name()));

    // empty sort
    findAndRerankWithSort(collectionName, "{}")
        .body("$", responseIsError())
        .body(
            "errors[0].errorCode", is(SortException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT.name()));

    // null sort
    findAndRerankWithSort(collectionName, "null")
        .body("$", responseIsError())
        .body(
            "errors[0].errorCode", is(SortException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT.name()));
  }

  /**
   * Sends a findAndRerank command with the sort. The command has the rerankQuery and rerankOn
   * options, so the sort is checked before anything fails for missing them.
   *
   * @param sort The JSON for the sort, or <code>null</code> to send no sort.
   */
  private ValidatableResponse findAndRerankWithSort(String collectionName, String sort) {
    var sortField = sort == null ? "" : "\"sort\": %s,".formatted(sort);
    var commandTemplate =
        """
        {"findAndRerank": {
            %s
            "options": {"rerankQuery": "hello", "rerankOn": "content"}
        }}
        """;
    return givenHeadersPostJsonThen(
        keyspaceName, collectionName, commandTemplate.formatted(sortField));
  }

  private void errorOnNotEnabled(
      String collectionName, String collectionSpec, String errorCode, String errorMessageContains) {

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
    errorOnNotEnabled(collectionName, collectionSpec, errorCode, errorMessageContains, rerank);
  }

  private void errorOnNotEnabled(
      String collectionName,
      String collectionSpec,
      String errorCode,
      String errorMessageContains,
      String rerankCommand) {
    createCollectionWithCleanup(collectionName, collectionSpec);

    givenHeadersPostJsonThen(keyspaceName, collectionName, rerankCommand)
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

  // Any non-null `"rerank"` payload is treated as an override attempt and validated; `{}` and
  // payloads whose fields are all explicit nulls both surface INVALID_RERANK_OVERRIDE rather
  // than silently falling back to the collection's reranking config — so a client mistake like
  // `{"provider": null}` cannot be misread as "no override".
  @Test
  void failOnRerankOverrideEmptyObject() {
    String collectionName = "rerank_override_empty";
    createCollectionWithCleanup(collectionName, VECTORIZE_NO_RERANK_SPEC);

    givenHeadersPostJsonThen(keyspaceName, collectionName, findAndRerankWithOverride("{}"))
        .body("$", responseIsError())
        .body("errors[0].errorCode", is(RequestException.Code.INVALID_RERANK_OVERRIDE.name()))
        .body("errors[0].message", containsString("Provider name is required"));
  }

  @Test
  void failOnRerankOverrideAllExplicitNullFields() {
    String collectionName = "rerank_override_null_fields";
    createCollectionWithCleanup(collectionName, VECTORIZE_NO_RERANK_SPEC);

    givenHeadersPostJsonThen(
            keyspaceName,
            collectionName,
            findAndRerankWithOverride(
                """
                {"provider": null, "modelName": null}
                """))
        .body("$", responseIsError())
        .body("errors[0].errorCode", is(RequestException.Code.INVALID_RERANK_OVERRIDE.name()))
        .body("errors[0].message", containsString("Provider name is required"));
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
  void failOnRerankOverrideMissingProvider() {
    String collectionName = "rerank_override_no_provider";
    createCollectionWithCleanup(collectionName, VECTORIZE_NO_RERANK_SPEC);

    givenHeadersPostJsonThen(
            keyspaceName,
            collectionName,
            findAndRerankWithOverride(
                """
                {"modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2"}
                """))
        .body("$", responseIsError())
        .body("errors[0].errorCode", is(RequestException.Code.INVALID_RERANK_OVERRIDE.name()))
        .body("errors[0].message", containsString("Provider name is required"));
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

  // Collection with both vectorize and rerank enabled (using the default reranking provider/model).
  // Used to verify the override path is applied even when the collection already has working
  // rerank config — i.e. the override is not silently ignored in favor of the collection default.
  private static final String VECTORIZE_AND_RERANK_SPEC =
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
            "enabled": true
          }
        }
      }
      """;

  /**
   * Positive verification that the override replaces the collection's reranking config. The
   * collection has valid rerank set up; if the override were silently ignored, the request would
   * pass validation and fail later at the embedding step. Sending an unknown provider in the
   * override surfaces INVALID_RERANK_OVERRIDE, proving the override path is taken even when the
   * collection's own rerank config would have worked.
   */
  @Test
  void overrideIsAppliedEvenWhenCollectionRerankEnabled() {
    String collectionName = "rerank_override_on_enabled_collection_bad";
    createCollectionWithCleanup(collectionName, VECTORIZE_AND_RERANK_SPEC);

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

  /**
   * Companion to {@link #overrideIsAppliedEvenWhenCollectionRerankEnabled}: a valid override on a
   * rerank-enabled collection also passes validation and proceeds to the embedding step (failing
   * with EMBEDDING_PROVIDER_CLIENT_ERROR for the same reason as {@link
   * #overrideWithProviderAndModelPassesValidation}).
   */
  @Test
  void validOverridePassesValidationOnRerankEnabledCollection() {
    String collectionName = "rerank_override_on_enabled_collection_ok";
    createCollectionWithCleanup(collectionName, VECTORIZE_AND_RERANK_SPEC);

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
