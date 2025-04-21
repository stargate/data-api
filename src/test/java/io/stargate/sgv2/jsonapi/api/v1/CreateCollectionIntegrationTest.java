package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class CreateCollectionIntegrationTest extends AbstractKeyspaceIntegrationTestBase {
  @Nested
  @Order(1)
  class CreateCollection {
    String createNonVectorCollectionJson =
        """
                    {
                      "createCollection": {
                        "name": "simple_collection"
                      }
                    }
                    """;

    String createVectorCollection =
        """
                    {
                      "createCollection": {
                        "name": "simple_collection",
                        "options" : {
                          "vector" : {
                          "size" : 5,
                            "function" : "cosine"
                          }
                        }
                      }
                    }
                    """;
    String createVectorCollectionWithOtherSizeSettings =
        """
                    {
                      "createCollection": {
                        "name": "simple_collection",
                        "options" : {
                          "vector" : {
                          "size" : 6,
                            "function" : "cosine"
                          }
                        }
                      }
                    }
                    """;
    String createVectorCollectionWithOtherFunctionSettings =
        """
                    {
                      "createCollection": {
                        "name": "simple_collection",
                        "options" : {
                          "vector" : {
                          "size" : 5,
                            "function" : "euclidean"
                          }
                        }
                      }
                    }
                    """;

    @Test
    public void happyPath() {
      final String collectionName = "col" + RandomStringUtils.randomNumeric(16);
      String json =
              """
                      {
                        "createCollection": {
                          "name": "%s"
                        }
                      }
                      """
              .formatted(collectionName);

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
      deleteCollection(collectionName);
    }

    @Test
    public void caseSensitive() {
      String json =
              """
                      {
                        "createCollection": {
                          "name": "%s"
                        }
                      }
                      """
              .formatted("testcollection");

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      json =
              """
                      {
                        "createCollection": {
                          "name": "%s"
                        }
                      }
                      """
              .formatted("testCollection");

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
      deleteCollection("testcollection");
      deleteCollection("testCollection");
    }

    @Test
    public void duplicateNonVectorCollectionName() {
      // create a non vector collection
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createNonVectorCollectionJson)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // recreate the same non vector collection
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createNonVectorCollectionJson)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // create a vector collection with the same name
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createVectorCollection)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .body("$", responseIsError())
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("EXISTING_COLLECTION_DIFFERENT_SETTINGS"))
          .body(
              "errors[0].message",
              containsString(
                  "trying to create Collection ('simple_collection') with different settings"));

      deleteCollection("simple_collection");
    }

    @Test
    public void duplicateVectorCollectionName() {
      // create a vector collection
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createVectorCollection)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
      // recreate the same vector collection
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createVectorCollection)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
      // create a non vector collection with the same name
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createNonVectorCollectionJson)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .body("$", responseIsError())
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("EXISTING_COLLECTION_DIFFERENT_SETTINGS"))
          .body(
              "errors[0].message",
              containsString(
                  "trying to create Collection ('simple_collection') with different settings"));

      deleteCollection("simple_collection");
    }

    @Test
    public void duplicateVectorCollectionNameWithDiffSetting() {
      // create a vector collection
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createVectorCollection)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
      // create another vector collection with the same name but different size setting
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createVectorCollectionWithOtherSizeSettings)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("EXISTING_COLLECTION_DIFFERENT_SETTINGS"))
          .body(
              "errors[0].message",
              containsString(
                  "trying to create Collection ('simple_collection') with different settings"));

      // create another vector collection with the same name but different function setting
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createVectorCollectionWithOtherFunctionSettings)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("EXISTING_COLLECTION_DIFFERENT_SETTINGS"))
          .body(
              "errors[0].message",
              containsString(
                  "trying to create Collection ('simple_collection') with different settings"));

      deleteCollection("simple_collection");
    }

    @Test
    public void happyCreateCollectionWithIndexingAllow() {
      final String createCollectionRequest =
          """
                      {
                        "createCollection": {
                          "name": "simple_collection_allow_indexing",
                          "options" : {
                            "vector" : {
                              "size" : 5,
                              "function" : "cosine"
                            },
                            "indexing" : {
                              "allow" : ["field1", "field2", "address.city", "_id", "$vector", "pricing.price&.usd"]
                            }
                          }
                        }
                      }
                      """;

      // create vector collection with indexing allow option
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionRequest)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // Also: should be idempotent so try creating again
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionRequest)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection("simple_collection_allow_indexing");
    }

    @Test
    public void happyCreateCollectionWithIndexingDeny() {
      // create vector collection with indexing deny option
      final String createCollectionRequest =
          """
                      {
                        "createCollection": {
                          "name": "simple_collection_deny_indexing",
                          "options" : {
                            "vector" : {
                              "size" : 5,
                              "function" : "cosine"
                            },
                            "indexing" : {
                              "deny" : ["field1", "field2", "address.city", "_id"]
                            }
                          }
                        }
                      }
                      """;

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionRequest)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // Also: should be idempotent so try creating again
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionRequest)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection("simple_collection_deny_indexing");
    }

    // Test to ensure single "*" accepted for "allow" or "deny" but not both
    @Test
    public void createCollectionWithIndexingStar() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                              {
                                "createCollection": {
                                  "name": "simple_collection_indexing_allow_star",
                                  "options" : {
                                    "indexing" : {
                                      "allow" : ["*"]
                                    }
                                  }
                                }
                              }
                              """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
      deleteCollection("simple_collection_indexing_allow_star");

      // create vector collection with indexing deny option
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                              {
                                "createCollection": {
                                  "name": "simple_collection_indexing_deny_star",
                                  "options" : {
                                    "indexing" : {
                                      "deny" : ["*"]
                                    }
                                  }
                                }
                              }
                              """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
      deleteCollection("simple_collection_indexing_deny_star");

      // And then check that we can't use both
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                              {
                                "createCollection": {
                                  "name": "simple_collection_indexing_deny_allow_star",
                                  "options" : {
                                    "indexing" : {
                                      "allow" : ["*"],
                                      "deny" : ["*"]
                                    }
                                  }
                                }
                              }
                              """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body(
              "errors[0].message",
              is("Invalid indexing definition: `allow` and `deny` cannot be used together"))
          .body("errors[0].errorCode", is("INVALID_INDEXING_DEFINITION"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }
  }

  @Nested
  @Order(2)
  class CreateCollectionFail {
    @Test
    public void failCreateCollectionWithIndexHavingDuplicates() {
      // create vector collection with error indexing option
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                      {
                        "createCollection": {
                          "name": "simple_collection_error1",
                          "options" : {
                            "indexing" : {
                              "allow" : ["field1", "field1"]
                            }
                          }
                        }
                      }
                      """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body(
              "errors[0].message",
              is("Invalid indexing definition: `allow` cannot contain duplicates"))
          .body("errors[0].errorCode", is("INVALID_INDEXING_DEFINITION"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void failCreateCollectionWithIndexHavingAllowAndDeny() {
      // create vector collection with error indexing option
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                {
                  "createCollection": {
                    "name": "simple_collection_error2",
                    "options" : {
                      "indexing" : {
                        "allow" : ["field1", "field2"],
                        "deny" : ["field1", "field2"]
                      }
                    }
                  }
                }
                """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body(
              "errors[0].message",
              is("Invalid indexing definition: `allow` and `deny` cannot be used together"))
          .body("errors[0].errorCode", is("INVALID_INDEXING_DEFINITION"))
          .body("errors[0].exceptionClass", is("JsonApiException"));

      deleteCollection("simple_collection");
    }

    @Test
    public void failWithInvalidNameInIndexingDeny() {
      // create a vector collection
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              // Dollars not allowed in regular field names (can only start operators)
              """
                    {
                      "createCollection": {
                        "name": "collection_with_bad_deny",
                        "options" : {
                          "indexing" : {
                            "deny" : ["field", "$in"]
                          }
                        }
                      }
                    }
                    """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body(
              "errors[0].message",
              startsWith("Invalid indexing definition: path must not start with '$'"))
          .body("errors[0].errorCode", is("INVALID_INDEXING_DEFINITION"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void failWithEmptyNameInIndexingDeny() {
      givenHeadersPostJsonThenOk(
              """
                    {
                      "createCollection": {
                        "name": "collection_with_bad_deny",
                        "options" : {
                          "indexing" : {
                            "deny" : ["field", ""]
                          }
                        }
                      }
                    }
                    """)
          .body("$", responseIsError())
          .body(
              "errors[0].message",
              startsWith(
                  "Invalid indexing definition: path must be represented as a non-empty string"))
          .body("errors[0].errorCode", is("INVALID_INDEXING_DEFINITION"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void failWithInvalidEscapeCharacterInIndexingDeny() {
      givenHeadersPostJsonThenOk(
              """
                    {
                      "createCollection": {
                        "name": "collection_with_bad_deny",
                        "options" : {
                          "indexing" : {
                            "deny" : ["field", "pricing.price&usd"]
                          }
                        }
                      }
                    }
                    """)
          .body("$", responseIsError())
          .body(
              "errors[0].message",
              startsWith(
                  "Invalid indexing definition: indexing path ('pricing.price&usd') is not a valid path."))
          .body("errors[0].errorCode", is("INVALID_INDEXING_DEFINITION"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void failWithInvalidMainLevelOption() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                            {
                              "createCollection": {
                                "name": "collection_with_invalid_option",
                                "options" : {
                                  "InDex" : {}
                                }
                              }
                            }
                            """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: No option \"InDex\" exists for `createCollection.options` (valid options: \"defaultId\", \"indexing\", \"lexical\", \"rerank\", \"vector\")"));
    }

    @Test
    public void failWithInvalidIdConfigOption() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                                    {
                                      "createCollection": {
                                        "name": "collection_with_invalid_idconfig",
                                        "options" : {
                                          "defaultId" : {
                                            "unknown": 3
                                          }
                                        }
                                      }
                                    }
                                    """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: Unrecognized field \"unknown\" for `createCollection.options.defaultId`"));
    }

    @Test
    public void failWithInvalidIndexingConfigOption() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                                    {
                                      "createCollection": {
                                        "name": "collection_with_invalid_indexconfig",
                                        "options" : {
                                          "indexing" : {
                                            "unknown": 3
                                          }
                                        }
                                      }
                                    }
                                    """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: Unrecognized field \"unknown\" for `createCollection.options.indexing` (known fields"));
    }

    @Test
    public void failWithInvalidVectorConfigOption() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                                    {
                                      "createCollection": {
                                        "name": "collection_with_invalid_vectorconfig",
                                        "options" : {
                                          "vector" : {
                                            "unknown": 3
                                          }
                                        }
                                      }
                                    }
                                    """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: Unrecognized field \"unknown\" for `createCollection.options.vector` (known fields"));
    }
  }

  @Nested
  @Order(3)
  class CreateCollectionWithEmbeddingServiceTestModelsAndProviders {
    @Test
    public void happyEmbeddingService() {
      final String createCollectionRequest =
          """
            {
                "createCollection": {
                    "name": "collection_with_vector_service",
                    "options": {
                        "vector": {
                            "metric": "cosine",
                            "dimension": 512,
                            "service": {
                                "provider": "azureOpenAI",
                                "modelName": "text-embedding-3-small",
                                "parameters": {
                                    "resourceName" : "vectorize",
                                    "deploymentId" : "vectorize"
                                }
                            }
                        }
                    }
                }
            }
          """;

      // create vector collection with vector service
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionRequest)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // Also: should be idempotent so try creating again
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionRequest)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");
    }

    @Test
    public void failProviderNotSupport() {
      // create a collection with embedding service provider not support
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                          {
                              "createCollection": {
                                  "name": "collection_with_vector_service",
                                  "options": {
                                      "vector": {
                                          "metric": "cosine",
                                          "dimension": 768,
                                          "service": {
                                              "provider": "test",
                                              "modelName": "textembedding-gecko@003",
                                              "authentication": {
                                                  "providerKey" : "shared_creds.providerKey"
                                              },
                                              "parameters": {
                                                  "projectId": "test"
                                              }
                                          }
                                      }
                                  }
                              }
                          }
                          """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: Service provider 'test' is not supported"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void failUnsupportedModel() {
      // create a collection with unsupported model name
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                            {
                                "createCollection": {
                                    "name": "collection_with_vector_service",
                                    "options": {
                                        "vector": {
                                            "metric": "cosine",
                                            "dimension": 768,
                                            "service": {
                                                "provider": "azureOpenAI",
                                                "modelName": "testModel",
                                                "parameters": {
                                                    "resourceName" : "vectorize",
                                                    "deploymentId" : "vectorize"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                                    """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: Model name 'testModel' for provider 'azureOpenAI' is not supported"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }
  }

  @Test
  public void failDeprecatedEmbedModel() {
    given()
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(
            """

                        {
                            "createCollection": {
                                "name": "collection_deprecated_nvidia_model",
                                "options": {
                                    "vector": {
                                        "dimension": 123,
                                        "service": {
                                            "provider": "nvidia",
                                            "modelName": "a-deprecated-nvidia-embedding-model"
                                        }
                                    }
                                }
                            }
                        }
                        """)
        .when()
        .post(KeyspaceResource.BASE_PATH, keyspaceName)
        .then()
        .statusCode(200)
        .body("$", responseIsError())
        .body(
            "errors[0].message",
            containsString("The model a-deprecated-nvidia-embedding-model is at DEPRECATED status"))
        .body("errors[0].errorCode", is("UNSUPPORTED_PROVIDER_MODEL"));
  }

  @Nested
  @Order(4)
  class CreateCollectionWithEmbeddingServiceTestDimension {
    @Test
    public void happyFixDimensionAutoPopulate() {
      final String createCollectionWithoutDimension =
          """
                          {
                              "createCollection": {
                                  "name": "collection_with_vector_service",
                                  "options": {
                                      "vector": {
                                          "metric": "cosine",
                                          "service": {
                                              "provider": "openai",
                                              "modelName": "text-embedding-3-small"
                                          }
                                      }
                                  }
                              }
                          }
                              """;
      final String createCollectionWithDimension =
          """
                          {
                              "createCollection": {
                                  "name": "collection_with_vector_service",
                                  "options": {
                                      "vector": {
                                          "metric": "cosine",
                                          "dimension": 1536,
                                          "service": {
                                              "provider": "openai",
                                              "modelName": "text-embedding-3-small"
                                          }
                                      }
                                  }
                              }
                          }
                              """;
      // create vector collection with vector service and no dimension
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionWithoutDimension)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // Also: should be idempotent when try creating with correct dimension
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionWithDimension)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");

      // create vector collection with vector service and correct dimension
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionWithDimension)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // Also: should be idempotent when try creating with no dimension
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionWithoutDimension)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");
    }

    @Test
    public void failNoServiceProviderAndNoDimension() {
      // create a collection with no dimension and service
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                            {
                                "createCollection": {
                                    "name": "collection_with_vector_service",
                                    "options": {
                                        "vector": {
                                            "metric": "cosine"
                                        }
                                    }
                                }
                            }
                            """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: The 'dimension' can not be null if 'service' is not provided"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void failFixDimensionUnmatchedVectorDimension() {
      // create a collection with unmatched vector dimension
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                            {
                                "createCollection": {
                                    "name": "collection_with_vector_service",
                                    "options": {
                                        "vector": {
                                            "metric": "cosine",
                                            "dimension": 5000,
                                            "service": {
                                                "provider": "openai",
                                                "modelName": "text-embedding-3-small"
                                            }
                                        }
                                    }
                                }
                            }
                                    """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: The provided dimension value '123' doesn't match the model's supported dimension value '1024'"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void happyRangeDimensionAutoPopulate() {
      final String createCollectionWithoutDimension =
          """
                  {
                      "createCollection": {
                          "name": "collection_with_vector_service",
                          "options": {
                              "vector": {
                                  "metric": "cosine",
                                  "service": {
                                      "provider": "openai",
                                      "modelName": "text-embedding-3-small"
                                  }
                              }
                          }
                      }
                  }
                      """;
      final String createCollectionWithDefaultDimension =
          """
                  {
                      "createCollection": {
                          "name": "collection_with_vector_service",
                          "options": {
                              "vector": {
                                  "metric": "cosine",
                                  "dimension": 1536,
                                  "service": {
                                      "provider": "openai",
                                      "modelName": "text-embedding-3-small"
                                  }
                              }
                          }
                      }
                  }
                      """;
      // create vector collection with vector service and no dimension
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionWithoutDimension)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // Also: should be idempotent when try creating with correct dimension
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionWithDefaultDimension)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");

      // create vector collection with vector service and correct dimension
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionWithDefaultDimension)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // Also: should be idempotent when try creating with no dimension
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionWithoutDimension)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");
    }

    @Test
    public void happyRangeDimensionInRange() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                            {
                                "createCollection": {
                                    "name": "collection_with_vector_service",
                                    "options": {
                                        "vector": {
                                            "metric": "cosine",
                                            "dimension": 512,
                                            "service": {
                                                "provider": "openai",
                                                "modelName": "text-embedding-3-small"
                                            }
                                        }
                                    }
                                }
                            }
                                    """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");
    }

    @Test
    public void failRangeDimensionNotInRange() {
      // create a collection with a dimension lower than the min
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                            {
                                "createCollection": {
                                    "name": "collection_with_vector_service",
                                    "options": {
                                        "vector": {
                                            "metric": "cosine",
                                            "dimension": 1,
                                            "service": {
                                                "provider": "openai",
                                                "modelName": "text-embedding-3-small"
                                            }
                                        }
                                    }
                                }
                            }
                                    """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: The provided dimension value (1) is not within the supported numeric range [2, 1536]"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"));

      // create a collection with a dimension higher than the min
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                            {
                                "createCollection": {
                                    "name": "collection_with_vector_service",
                                    "options": {
                                        "vector": {
                                            "metric": "cosine",
                                            "dimension": 2000,
                                            "service": {
                                                "provider": "openai",
                                                "modelName": "text-embedding-3-small"
                                            }
                                        }
                                    }
                                }
                            }
                                    """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: The provided dimension value (2000) is not within the supported numeric range [2, 1536]"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }
  }

  @Nested
  @Order(5)
  class CreateCollectionWithEmbeddingServiceTestAuth {
    @Test
    public void happyWithNoneAuth() {
      // create a collection without providing authentication
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                            {
                                "createCollection": {
                                    "name": "collection_with_vector_service",
                                    "options": {
                                        "vector": {
                                            "metric": "cosine",
                                            "dimension": 1024,
                                            "service": {
                                                "provider": "nvidia",
                                                "modelName": "NV-Embed-QA"
                                            }
                                        }
                                    }
                                }
                            }
                                """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");
    }

    @Test
    public void failNotExistAuthKey() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                            {
                                "createCollection": {
                                    "name": "collection_with_vector_service",
                                    "options": {
                                        "vector": {
                                            "metric": "cosine",
                                            "dimension": 1024,
                                            "service": {
                                                "provider": "nvidia",
                                                "modelName": "NV-Embed-QA",
                                                "authentication": {
                                                    "providerKey": "shared_creds.providerKey"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: Service provider 'nvidia' does not support authentication key 'providerKey'"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void failNoneAndHeaderDisabled() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                                    {
                                        "createCollection": {
                                            "name": "collection_with_vector_service",
                                            "options": {
                                                "vector": {
                                                    "metric": "cosine",
                                                    "dimension": 1536,
                                                    "service": {
                                                        "provider": "huggingface",
                                                        "modelName": "sentence-transformers/all-MiniLM-L6-v2"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: Service provider 'huggingface' does not support either 'NONE' or 'HEADER' authentication types."))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void failInvalidAuthKey() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                                    {
                                        "createCollection": {
                                            "name": "collection_with_vector_service",
                                            "options": {
                                                "vector": {
                                                    "metric": "cosine",
                                                    "dimension": 1536,
                                                    "service": {
                                                        "provider": "openai",
                                                        "modelName": "text-embedding-ada-002",
                                                        "authentication": {
                                                            "test": "shared_creds.providerKey"
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: Service provider 'openai' does not support authentication key 'test'"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void happyValidAuthKey() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                    {
                        "createCollection": {
                            "name": "collection_with_vector_service",
                            "options": {
                                "vector": {
                                    "metric": "cosine",
                                    "dimension": 1536,
                                    "service": {
                                        "provider": "openai",
                                        "modelName": "text-embedding-ada-002"
                                    }
                                }
                            }
                        }
                    }
                    """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");
    }

    @Test
    public void happyProviderKeyFormat() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                        {
                            "createCollection": {
                                "name": "collection_with_vector_service",
                                "options": {
                                    "vector": {
                                        "metric": "cosine",
                                        "dimension": 1536,
                                        "service": {
                                            "provider": "openai",
                                            "modelName": "text-embedding-ada-002",
                                            "authentication": {
                                                "providerKey" : "shared_creds"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                                {
                                    "createCollection": {
                                        "name": "collection_with_vector_service",
                                        "options": {
                                            "vector": {
                                                "metric": "cosine",
                                                "dimension": 1536,
                                                "service": {
                                                    "provider": "openai",
                                                    "modelName": "text-embedding-ada-002",
                                                    "authentication": {
                                                        "providerKey" : "shared_creds.providerKey"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");
    }
  }

  @Nested
  @Order(6)
  class CreateCollectionWithEmbeddingServiceTestParameters {
    @Test
    public void failWithMissingRequiredProviderParameters() {
      // create a collection without providing required parameters
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                            {
                                "createCollection": {
                                    "name": "collection_with_vector_service",
                                    "options": {
                                        "vector": {
                                            "metric": "cosine",
                                            "dimension": 768,
                                            "service": {
                                                "provider": "azureOpenAI",
                                                "modelName": "text-embedding-3-small"
                                            }
                                        }
                                    }
                                }
                            }
                            """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: Required parameter 'resourceName' for the provider 'azureOpenAI' missing"));
    }

    @Test
    public void failWithUnrecognizedProviderParameters() {
      // create a collection with unrecognized parameters
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                            {
                                "createCollection": {
                                    "name": "collection_with_vector_service",
                                    "options": {
                                        "vector": {
                                            "metric": "cosine",
                                            "dimension": 768,
                                            "service": {
                                                "provider": "azureOpenAI",
                                                "modelName": "text-embedding-3-small",
                                                "parameters": {
                                                    "test": "test"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                                    """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: Unexpected parameter 'test' for the provider 'azureOpenAI' provided"));
    }

    @Test
    public void failWithUnexpectedProviderParameters() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                            {
                                "createCollection": {
                                    "name": "collection_with_vector_service",
                                    "options": {
                                        "vector": {
                                            "metric": "cosine",
                                            "dimension": 768,
                                            "service": {
                                                "provider": "openai",
                                                "modelName": "text-embedding-3-small",
                                                "parameters": {
                                                    "test": "test"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                                    """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: Unexpected parameter 'test' for the provider 'openai' provided"));
    }

    @Test
    public void failWithWrongProviderParameterType() {
      // create a collection with wrong parameter type
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                            {
                                "createCollection": {
                                    "name": "collection_with_vector_service",
                                    "options": {
                                        "vector": {
                                            "metric": "cosine",
                                            "dimension": 768,
                                            "service": {
                                                "provider": "azureOpenAI",
                                                "modelName": "text-embedding-3-small",
                                                "parameters": {
                                                    "resourceName": 123,
                                                    "deploymentId": "vectorize"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                                    """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: The provided parameter 'resourceName' type is incorrect. Expected: 'string'"));
    }

    @Test
    public void failWithMissingModelParameters() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                            {
                                "createCollection": {
                                    "name": "collection_with_vector_service",
                                    "options": {
                                        "vector": {
                                            "metric": "cosine",
                                            "dimension": 768,
                                            "service": {
                                                "provider": "vertexai",
                                                "modelName": "textembedding-gecko@003",
                                                "parameters": {
                                                    "projectId": "test"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: Required parameter 'autoTruncate' for the provider 'vertexai' missing"));
    }

    @Test
    public void failWithUnexpectedModelParameters() {
      // create a collection with unrecognized parameters
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                            {
                                "createCollection": {
                                    "name": "collection_with_vector_service",
                                    "options": {
                                        "vector": {
                                            "metric": "cosine",
                                            "dimension": 768,
                                            "service": {
                                                "provider": "azureOpenAI",
                                                "modelName": "text-embedding-3-small",
                                                "parameters": {
                                                    "resourceName": "vectorize",
                                                    "deploymentId": "vectorize",
                                                    "vectorDimension": 512
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: Unexpected parameter 'vectorDimension' for the provider 'azureOpenAI' provided"));
    }

    @Test
    public void failWithWrongModelParameterType() {
      // create a collection with wrong parameter type
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                            {
                                "createCollection": {
                                    "name": "collection_with_vector_service",
                                    "options": {
                                        "vector": {
                                            "metric": "cosine",
                                            "dimension": 768,
                                            "service": {
                                                "provider": "azureOpenAI",
                                                "modelName": "text-embedding-3-small",
                                                "parameters": {
                                                    "resourceName": "vectorize",
                                                    "deploymentId": 123
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: The provided parameter 'deploymentId' type is incorrect. Expected: 'string'"));
    }
  }

  @Nested
  @Order(7)
  class CreateCollectionWithSourceModel {
    @Test
    public void happyWithSourceModelAndMetrics() {
      // create a collection with source model and metric
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                  {
                      "createCollection": {
                          "name": "collection_with_sourceModel_metric",
                          "options": {
                              "vector": {
                                  "metric": "cosine",
                                  "sourceModel": "openai-v3-small",
                                  "dimension": 1536,
                                  "service": {
                                      "provider": "openai",
                                      "modelName": "text-embedding-3-small"
                                  }
                              }
                          }
                      }
                  }
                  """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // verify the collection using FindCollection
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                      {
                        "findCollections": {
                            "options" : {
                                "explain": true
                            }
                         }
                      }
                      """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.collections", hasSize(1))
          .body("status.collections[0].options.vector.metric", is("cosine"))
          .body("status.collections[0].options.vector.sourceModel", is("openai-v3-small"));

      deleteCollection("collection_with_sourceModel_metric");
    }

    @Test
    public void happyWithSourceModelOnly() {
      // create a collection with source model - metric will be auto-populated to 'dot_product'
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                          {
                              "createCollection": {
                                  "name": "collection_with_sourceModel",
                                  "options": {
                                      "vector": {
                                          "sourceModel": "openai-v3-small",
                                          "dimension": 1536,
                                          "service": {
                                              "provider": "openai",
                                              "modelName": "text-embedding-3-small"
                                          }
                                      }
                                  }
                              }
                          }
                          """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // verify the collection using FindCollection
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                      {
                        "findCollections": {
                            "options" : {
                                "explain": true
                            }
                         }
                      }
                      """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.collections", hasSize(1))
          .body("status.collections[0].options.vector.metric", is("dot_product"))
          .body("status.collections[0].options.vector.sourceModel", is("openai-v3-small"));

      deleteCollection("collection_with_sourceModel");
    }

    @Test
    public void happyWithMetricOnly() {
      // create a collection with metric - source model will be auto-populated to 'other'
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                    {
                        "createCollection": {
                            "name": "collection_with_metric",
                            "options": {
                                "vector": {
                                    "metric": "cosine",
                                    "dimension": 1536,
                                    "service": {
                                        "provider": "openai",
                                        "modelName": "text-embedding-3-small"
                                    }
                                }
                            }
                        }
                    }
                    """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // verify the collection using FindCollection
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                          {
                            "findCollections": {
                                "options" : {
                                    "explain": true
                                }
                             }
                          }
                          """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.collections", hasSize(1))
          .body("status.collections[0].options.vector.metric", is("cosine"))
          .body("status.collections[0].options.vector.sourceModel", is("other"));

      deleteCollection("collection_with_metric");
    }

    @Test
    public void happyNoSourceModelAndMetric() {
      // create a collection without sourceModel and metric - source model will be auto-populated to
      // 'other' and metric to 'cosine'
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                      {
                          "createCollection": {
                              "name": "collection_with_no_sourceModel_metric",
                              "options": {
                                  "vector": {
                                      "dimension": 1536,
                                      "service": {
                                          "provider": "openai",
                                          "modelName": "text-embedding-3-small"
                                      }
                                  }
                              }
                          }
                      }
                      """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // verify the collection using FindCollection
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                  {
                    "findCollections": {
                        "options" : {
                            "explain": true
                        }
                     }
                  }
                  """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.collections", hasSize(1))
          .body("status.collections[0].options.vector.metric", is("cosine"))
          .body("status.collections[0].options.vector.sourceModel", is("other"));

      deleteCollection("collection_with_no_sourceModel_metric");
    }

    @Test
    public void failWithInvalidSourceModel() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                      {
                          "createCollection": {
                              "name": "collection_with_sourceModel",
                              "options": {
                                  "vector": {
                                      "sourceModel": "invalidName",
                                      "dimension": 1536,
                                      "service": {
                                          "provider": "openai",
                                          "modelName": "text-embedding-3-small"
                                      }
                                  }
                              }
                          }
                      }
                      """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("COMMAND_FIELD_INVALID"))
          .body(
              "errors[0].message",
              startsWith(
                  "Request invalid: field 'command.options.vector.sourceModel' value \"invalidName\" not valid."));
    }

    @Test
    public void failWithInvalidSourceModelObject() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                              {
                                  "createCollection": {
                                      "name": "collection_with_sourceModel",
                                      "options": {
                                          "vector": {
                                              "sourceModel": "invalidName",
                                              "dimension": 1536,
                                              "service": {
                                                  "provider": "openai",
                                                  "modelName": "text-embedding-3-small"
                                              }
                                          }
                                      }
                                  }
                              }
                              """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("COMMAND_FIELD_INVALID"))
          .body(
              "errors[0].message",
              startsWith(
                  "Request invalid: field 'command.options.vector.sourceModel' value \"invalidName\" not valid."));
    }
  }

  @Nested
  @Order(99)
  class Metrics {
    @Test
    public void checkMetrics() {
      CreateCollectionIntegrationTest.super.checkMetrics("CreateCollectionCommand");
      CreateCollectionIntegrationTest.super.checkDriverMetricsTenantId();
    }
  }
}
