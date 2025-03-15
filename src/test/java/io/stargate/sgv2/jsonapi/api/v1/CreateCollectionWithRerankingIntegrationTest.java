package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsDDLSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class CreateCollectionWithRerankingIntegrationTest
    extends AbstractKeyspaceIntegrationTestBase {
  @Nested
  @Order(1)
  class CreateRerankingHappyPath {
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
                                          "lexical": {
                                              "enabled": true,
                                              "analyzer": "standard"
                                          },
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
                      .formatted(collectionName)));

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
                                                  "lexical": {
                                                      "enabled": true,
                                                      "analyzer": "standard"
                                                  },
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
                      .formatted(collectionName)));

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
                                                  "lexical": {
                                                      "enabled": true,
                                                      "analyzer": "standard"
                                                  },
                                                  "rerank": {
                                                      "enabled": false
                                                  }
                                              }
                                          }
                              """
                      .formatted(collectionName)));

      deleteCollection(collectionName);
    }

    @Test
    void createRerankingWithDisabledAndModel() {
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
                                                    "lexical": {
                                                        "enabled": true,
                                                        "analyzer": "standard"
                                                    },
                                                    "rerank": {
                                                        "enabled": false
                                                    }
                                                }
                                            }
                                """
                      .formatted(collectionName)));

      deleteCollection(collectionName);
    }
  }

  @Nested
  @Order(2)
  class CreateRerankingFail {
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
                  "The provided options are invalid: 'enabled' is required property for 'rerank'"));
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
                  "The provided options are invalid: 'provider' is required property for 'rerank.service' Object value"));
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
                  "The provided options are invalid: 'modelName' is needed for rerank provider nvidia"));
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
}
