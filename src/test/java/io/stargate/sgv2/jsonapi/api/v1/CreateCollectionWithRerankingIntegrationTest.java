package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsDDLSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class CreateCollectionWithRerankingIntegrationTest
    extends AbstractKeyspaceIntegrationTestBase {
  @Nested
  @Order(1)
  class CreateRerankingHappyPath {
    @Test
    void createNoRerankingConfigAndUseDefault() {
      final String collectionName = "coll_Reranking_" + RandomStringUtils.randomNumeric(16);
      String json =
              """
                          {
                            "createCollection": {
                              "name": "%s"
                            }
                          }
                          """
              .formatted(collectionName);

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // verify the collection using FindCollection
      givenHeadersPostJsonThenOkNoErrors(
              """
                              {
                                "findCollections": {
                                    "options" : {
                                        "explain": true
                                    }
                                 }
                              }
                              """)
          .body("$", responseIsDDLSuccess())
          .body("status.collections", hasSize(1))
          .body(
              "status.collections[0]",
              jsonEquals(
                      """
                      {
                                      "name": "%s",
                                      "options": {
                                          "lexical": %s,
                                          "rerank": {
                                              "enabled": true,
                                              "service": {
                                                  "provider": "nvidia",
                                                  "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2"
                                              }
                                          }
                                      }
                                  }
                      """
                      .formatted(collectionName, lexical())));

      deleteCollection(collectionName);
    }

    @Test
    void createRerankingSimpleEnabledMinimal() {
      final String collectionName = "coll_rerank_minimal" + RandomStringUtils.randomNumeric(16);
      String json = createRequestWithReranking(collectionName, "{\"enabled\": true}");

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // verify the collection using FindCollection
      givenHeadersPostJsonThenOkNoErrors(
              """
                              {
                                "findCollections": {
                                    "options" : {
                                        "explain": true
                                    }
                                 }
                              }
                              """)
          .body("$", responseIsDDLSuccess())
          .body("status.collections", hasSize(1))
          .body(
              "status.collections[0]",
              jsonEquals(
                      """
                      {
                                      "name": "%s",
                                      "options": {
                                          "lexical": %s,
                                          "rerank": {
                                              "enabled": true,
                                              "service": {
                                                  "provider": "nvidia",
                                                  "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2"
                                              }
                                          }
                                      }
                                  }
                      """
                      .formatted(collectionName, lexical())));

      deleteCollection(collectionName);
    }

    @Test
    void createRerankingSimpleEnabledStandard() {
      final String collectionName = "coll_Reranking_simple" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithReranking(
              collectionName,
              """
                           {
                               "enabled": true,
                               "service": {
                                   "provider": "nvidia",
                                   "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2"
                               }
                           }
                          """);

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // verify the collection using FindCollection
      givenHeadersPostJsonThenOkNoErrors(
              """
                              {
                                "findCollections": {
                                    "options" : {
                                        "explain": true
                                    }
                                 }
                              }
                              """)
          .body("$", responseIsDDLSuccess())
          .body("status.collections", hasSize(1))
          .body(
              "status.collections[0]",
              jsonEquals(
                      """
                              {
                                              "name": "%s",
                                              "options": {
                                                  "lexical": %s,
                                                  "rerank": {
                                                      "enabled": true,
                                                      "service": {
                                                          "provider": "nvidia",
                                                          "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2"
                                                      }
                                                  }
                                              }
                                          }
                              """
                      .formatted(collectionName, lexical())));

      deleteCollection(collectionName);
    }

    @Test
    void createRerankingSimpleDisabled() {
      final String collectionName = "coll_Reranking_disabled" + RandomStringUtils.randomNumeric(16);
      String json = createRequestWithReranking(collectionName, "{\"enabled\": false}");

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // verify the collection using FindCollection
      givenHeadersPostJsonThenOkNoErrors(
              """
                              {
                                "findCollections": {
                                    "options" : {
                                        "explain": true
                                    }
                                 }
                              }
                              """)
          .body("$", responseIsDDLSuccess())
          .body("status.collections", hasSize(1))
          .body(
              "status.collections[0]",
              jsonEquals(
                      """
                              {
                                  "name": "%s",
                                  "options": {
                                      "lexical": %s,
                                      "rerank": {
                                          "enabled": false
                                      }
                                  }
                              }
                              """
                      .formatted(collectionName, lexical())));

      deleteCollection(collectionName);
    }

    @Test
    void createRerankingDisabledWithEmptyService() {
      final String collectionName = "coll_Reranking_disabled" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithReranking(
              collectionName,
              """
                                        {
                                            "enabled": false,
                                            "service": {}
                                        }
                                        """);

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // verify the collection using FindCollection
      givenHeadersPostJsonThenOkNoErrors(
              """
                              {
                                "findCollections": {
                                    "options" : {
                                        "explain": true
                                    }
                                 }
                              }
                              """)
          .body("$", responseIsDDLSuccess())
          .body("status.collections", hasSize(1))
          .body(
              "status.collections[0]",
              jsonEquals(
                      """
                            {
                                "name": "%s",
                                "options": {
                                    "lexical": %s,
                                    "rerank": {
                                        "enabled": false
                                    }
                                }
                            }
                            """
                      .formatted(collectionName, lexical())));

      deleteCollection(collectionName);
    }
  }

  @Nested
  @Order(2)
  class CreateRerankingFail {
    @Test
    void failCreateRerankingWithDisabledAndModel() {
      final String collectionName = "coll_Reranking_disabled" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithReranking(
              collectionName,
              """
                              {
                                  "enabled": false,
                                  "service": {
                                      "provider": "nvidia",
                                      "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2"
                                  }
                              }
                              """);

      givenHeadersPostJsonThenOk(json)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              containsString(
                  "The provided options are invalid: 'rerank' is disabled, but 'rerank.service' configuration is provided"));
    }

    @Test
    void failCreateRerankingMissingEnabled() {
      final String collectionName = "coll_Reranking_" + RandomStringUtils.randomNumeric(16);
      String json = createRequestWithReranking(collectionName, "{ }");

      givenHeadersPostJsonThenOk(json)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              containsString(
                  "The provided options are invalid: 'enabled' is required property for 'rerank' Object value"));
    }

    @Test
    void failMissingServiceProvider() {
      final String collectionName = "coll_Reranking_" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithReranking(
              collectionName,
              """
                                    {
                                      "enabled": true,
                                      "service": {}
                                    }
                              """);

      givenHeadersPostJsonThenOk(json)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              containsString(
                  "The provided options are invalid: Provider name is required for reranking service configuration"));
    }

    @Test
    void failUnknownServiceProvider() {
      final String collectionName = "coll_Reranking_" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithReranking(
              collectionName,
              """
                              {
                                "enabled": true,
                                "service": {
                                    "provider": "unknown"
                                }
                              }
                              """);

      givenHeadersPostJsonThenOk(json)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              containsString(
                  "The provided options are invalid: Reranking provider 'unknown' is not supported"));
    }

    @Test
    void failMissingServiceModel() {
      final String collectionName = "coll_Reranking_" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithReranking(
              collectionName,
              """
                                      {
                                        "enabled": true,
                                        "service": {
                                            "provider": "nvidia"
                                        }
                                      }
                              """);

      givenHeadersPostJsonThenOk(json)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              containsString(
                  "The provided options are invalid: Model name is required for reranking provider 'nvidia'"));
    }

    @Test
    void failUnknownServiceModel() {
      final String collectionName = "coll_Reranking_" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithReranking(
              collectionName,
              """
                              {
                                "enabled": true,
                                "service": {
                                    "provider": "nvidia",
                                    "modelName": "unknown"
                                }
                              }
                              """);

      givenHeadersPostJsonThenOk(json)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              containsString(
                  "The provided options are invalid: Model 'unknown' is not supported by reranking provider 'nvidia'"));
    }

    @Test
    void failUnsupportedAuthentication() {
      final String collectionName = "coll_Reranking_" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithReranking(
              collectionName,
              """
                              {
                                "enabled": true,
                                "service": {
                                    "provider": "nvidia",
                                    "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2",
                                    "authentication": {
                                        "providerKey" : "myKey"
                                    }
                                }
                              }
                              """);

      givenHeadersPostJsonThenOk(json)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              containsString(
                  "The provided options are invalid: Reranking provider 'nvidia' currently only supports 'NONE' or 'HEADER' authentication types. No authentication parameters should be provided."));
    }

    @Test
    void failUnsupportedParameters() {
      final String collectionName = "coll_Reranking_" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithReranking(
              collectionName,
              """
                              {
                                "enabled": true,
                                "service": {
                                    "provider": "nvidia",
                                    "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2",
                                    "parameters": {
                                        "test": "test"
                                    }
                                }
                              }
                              """);

      givenHeadersPostJsonThenOk(json)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              containsString(
                  "Reranking provider 'nvidia' currently doesn't support any parameters. No parameters should be provided."));
    }

    private static Stream<Arguments> deprecatedRerankingModelSource() {
      return Stream.of(
          Arguments.of(
              "DEPRECATED",
              "nvidia/a-random-deprecated-model",
              SchemaException.Code.DEPRECATED_AI_MODEL),
          Arguments.of(
              "END_OF_LIFE",
              "nvidia/a-random-EOL-model",
              SchemaException.Code.END_OF_LIFE_AI_MODEL));
    }

    @ParameterizedTest
    @MethodSource("deprecatedRerankingModelSource")
    public void failDeprecatedEOLRerankModel(
        String status, String modelName, SchemaException.Code errorCode) {
      final String collectionName = "coll_Reranking_" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithReranking(
              collectionName,
                  """
                              {
                                "enabled": true,
                                "service": {
                                    "provider": "nvidia",
                                    "modelName": "%s"
                                }
                              }
                              """
                  .formatted(modelName));

      givenHeadersPostJsonThenOk(json)
          .body("$", responseIsError())
          .body(
              "errors[0].message",
              containsString("The model is: %s. It is at %s status".formatted(modelName, status)))
          .body("errors[0].errorCode", is(errorCode.name()));
    }
  }

  private String createRequestWithReranking(String collectionName, String rerankingDef) {
    return
        """
                          {
                            "createCollection": {
                              "name": "%s",
                              "options": {
                                "rerank": %s
                              }
                            }
                          }
                          """
        .formatted(collectionName, rerankingDef);
  }

  private String lexical() {
    return isLexicalAvailableForDB()
        ?
        """
            {
                "enabled": true,
                "analyzer": "standard"
            }
            """
        :
        """
            {
                "enabled": false
            }
            """;
  }
}
