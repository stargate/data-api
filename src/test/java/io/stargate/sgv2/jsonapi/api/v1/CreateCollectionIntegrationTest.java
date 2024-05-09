package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class CreateCollectionIntegrationTest extends AbstractNamespaceIntegrationTestBase {
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
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
      deleteCollection(collectionName);
    }

    @Test
    public void duplicateNonVectorCollectionName() {
      // create a non vector collection
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createNonVectorCollectionJson)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      // recreate the same non vector collection
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createNonVectorCollectionJson)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      // create a vector collection with the same name
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createVectorCollection)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              containsString("provided collection ('simple_collection') already exists with"))
          .body("errors[0].errorCode", is("INVALID_COLLECTION_NAME"))
          .body("errors[0].exceptionClass", is("JsonApiException"));

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
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
      // recreate the same vector collection
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createVectorCollection)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
      // create a non vector collection with the same name
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createNonVectorCollectionJson)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              containsString("provided collection ('simple_collection') already exists with"))
          .body("errors[0].errorCode", is("INVALID_COLLECTION_NAME"))
          .body("errors[0].exceptionClass", is("JsonApiException"));

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
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
      // create another vector collection with the same name but different size setting
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createVectorCollectionWithOtherSizeSettings)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              containsString("provided collection ('simple_collection') already exists with"))
          .body("errors[0].errorCode", is("INVALID_COLLECTION_NAME"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
      // create another vector collection with the same name but different function setting
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createVectorCollectionWithOtherFunctionSettings)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              containsString("provided collection ('simple_collection') already exists with"))
          .body("errors[0].errorCode", is("INVALID_COLLECTION_NAME"))
          .body("errors[0].exceptionClass", is("JsonApiException"));

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
                      "allow" : ["field1", "field2", "address.city", "_id", "$vector"]
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
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      // Also: should be idempotent so try creating again
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionRequest)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
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
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      // Also: should be idempotent so try creating again
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionRequest)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
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
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
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
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
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
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              is("Invalid indexing definition: `allow` and `deny` cannot be used together"))
          .body("errors[0].errorCode", is("INVALID_INDEXING_DEFINITION"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

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
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
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
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              is("Invalid indexing definition: `allow` and `deny` cannot be used together"))
          .body("errors[0].errorCode", is("INVALID_INDEXING_DEFINITION"))
          .body("errors[0].exceptionClass", is("JsonApiException"));

      deleteCollection("simple_collection");
    }

    @Test
    public void failWithInvalidNameInIndexingAllows() {
      // create a vector collection
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          // Brackets not allowed in field names
          .body(
              """
                    {
                      "createCollection": {
                        "name": "collection_with_bad_allows",
                        "options" : {
                          "indexing" : {
                            "allow" : ["valid-field", "address[1].street"]
                          }
                        }
                      }
                    }
                    """)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              startsWith(
                  "Invalid indexing definition: `allow` contains invalid path: 'address[1].street'"))
          .body("errors[0].errorCode", is("INVALID_INDEXING_DEFINITION"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
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
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              startsWith("Invalid indexing definition: `deny` contains invalid path: '$in'"))
          .body("errors[0].errorCode", is("INVALID_INDEXING_DEFINITION"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void failWithInvalidOption() {
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
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: No option \"InDex\" found as createCollectionCommand option (known options: \"defaultId\", \"indexing\", \"vector\")"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void happyCreateCollectionWithEmbeddingService() {
      final String createCollectionRequest =
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
                                      "authentication": {
                                          "x-embedding-api-key": "user_key"
                                      },
                                      "parameters": {
                                          "projectId": "test"
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
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      // Also: should be idempotent so try creating again
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionRequest)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");
    }

    @Test
    public void failCreateCollectionWithEmbeddingServiceProviderNotSupport() {
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
                                          "x-embedding-api-key": "user_key",
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
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: Service provider 'test' is not supported"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void failCreateCollectionWithEmbeddingServiceNotProvideRequiredParameters() {
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
                                        "provider": "vertexai",
                                        "modelName": "text-embedding-3-small",
                                        "authentication": {
                                            "x-embedding-api-key": "user_key"
                                        }
                                    }
                                }
                            }
                        }
                    }
                    """)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: Required parameter 'projectId' for the provider 'vertexai' missing"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void failCreateCollectionWithEmbeddingServiceWithUnconfiguredParameters() {
      // create a collection with unconfigured parameters
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
                                        "authentication": {
                                            "x-embedding-api-key": "user_key"
                                        },
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
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: Unexpected parameter 'test' for the provider 'vertexai' provided"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"));

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
                                        "authentication": {
                                            "x-embedding-api-key": "user_key"
                                        },
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
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: Parameters provided but the provider 'openai' expects none"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void failCreateCollectionWithEmbeddingServiceWrongParameterType() {
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
                                        "provider": "vertexai",
                                        "modelName": "textembedding-gecko@003",
                                        "authentication": {
                                            "x-embedding-api-key": "user_key"
                                        },
                                        "parameters": {
                                            "projectId": 123
                                        }
                                    }
                                }
                            }
                        }
                    }
                            """)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: The provided parameter 'projectId' type is incorrect. Expected: 'string'"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void failCreateCollectionWithEmbeddingServiceUnsupportedModel() {
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
                                        "provider": "vertexai",
                                        "modelName": "testModel",
                                        "authentication": {
                                            "x-embedding-api-key": "user_key"
                                        },
                                        "parameters": {
                                            "projectId": "123"
                                        }
                                    }
                                }
                            }
                        }
                    }
                            """)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: Model name 'testModel' for provider 'vertexai' is not supported"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }
  }

  @Nested
  @Order(2)
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
                                              "provider": "vertexai",
                                              "modelName": "textembedding-gecko@003",
                                              "authentication": {
                                                  "x-embedding-api-key": "user_key"
                                              },
                                              "parameters": {
                                                  "projectId": "test"
                                              }
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
                                          "dimension": 768,
                                          "service": {
                                              "provider": "vertexai",
                                              "modelName": "textembedding-gecko@003",
                                              "authentication": {
                                                  "x-embedding-api-key": "user_key"
                                              },
                                              "parameters": {
                                                  "projectId": "test"
                                              }
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
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      // Also: should be idempotent when try creating with correct dimension
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionWithDimension)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");

      // create vector collection with vector service and correct dimension
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionWithDimension)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      // Also: should be idempotent when try creating with no dimension
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionWithoutDimension)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
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
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
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
                                            "dimension": 123,
                                            "service": {
                                                "provider": "vertexai",
                                                "modelName": "textembedding-gecko@003",
                                                "authentication": {
                                                    "x-embedding-api-key": "user_key"
                                                },
                                                "parameters": {
                                                    "projectId": "123"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                                    """)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: The provided dimension value '123' doesn't match the model's supported dimension value '768'"))
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
                                      "modelName": "text-embedding-3-small",
                                      "authentication": {
                                          "x-embedding-api-key": "user_key"
                                      }
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
                                  "dimension": 512,
                                  "service": {
                                      "provider": "openai",
                                      "modelName": "text-embedding-3-small",
                                      "authentication": {
                                          "x-embedding-api-key": "user_key"
                                      }
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
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      // Also: should be idempotent when try creating with correct dimension
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionWithDefaultDimension)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");

      // create vector collection with vector service and correct dimension
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionWithDefaultDimension)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      // Also: should be idempotent when try creating with no dimension
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollectionWithoutDimension)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
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
                                                "modelName": "text-embedding-3-small",
                                                "authentication": {
                                                    "x-embedding-api-key": "user_key"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                                    """)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
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
                                                "modelName": "text-embedding-3-small",
                                                "authentication": {
                                                    "x-embedding-api-key": "user_key"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                                    """)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
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
                                                "modelName": "text-embedding-3-small",
                                                "authentication": {
                                                    "x-embedding-api-key": "user_key"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                                    """)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              startsWith(
                  "The provided options are invalid: The provided dimension value (2000) is not within the supported numeric range [2, 1536]"))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }
  }

  @Nested
  @Order(3)
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
              .post(NamespaceResource.BASE_PATH, namespaceName)
              .then()
              .statusCode(200)
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
              .post(NamespaceResource.BASE_PATH, namespaceName)
              .then()
              .statusCode(200)
              .body("status", is(nullValue()))
              .body("data", is(nullValue()))
              .body(
                      "errors[0].message",
                      startsWith(
                              "The provided options are invalid: Service provider 'nvidia' does not support authentication key 'providerKey'"))
              .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
              .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void failNoneDisabled() {
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
              .post(NamespaceResource.BASE_PATH, namespaceName)
              .then()
              .statusCode(200)
              .body("status", is(nullValue()))
              .body("data", is(nullValue()))
              .body(
                      "errors[0].message",
                      startsWith(
                              "The provided options are invalid: Service provider 'openai' does not support 'NONE' authentication"))
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
                                                            "providerKey": "shared_creds.providerKey"
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    """)
              .when()
              .post(NamespaceResource.BASE_PATH, namespaceName)
              .then()
              .statusCode(200)
              .body("status", is(nullValue()))
              .body("data", is(nullValue()))
              .body(
                      "errors[0].message",
                      startsWith(
                              "The provided options are invalid: Service provider 'openai' does not support authentication key 'providerKey'"))
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
                                                        "modelName": "text-embedding-ada-002",
                                                        "authentication": {
                                                            "x-embedding-api-key": "api_key"
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    """)
              .when()
              .post(NamespaceResource.BASE_PATH, namespaceName)
              .then()
              .statusCode(200)
              .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");
    }
  }

  private void deleteCollection(String collectionName) {
    given()
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(
                """
                {
                  "deleteCollection": {
                    "name": "%s"
                  }
                }
                """
                .formatted(collectionName))
        .when()
        .post(NamespaceResource.BASE_PATH, namespaceName)
        .then()
        .statusCode(200)
        .body("status.ok", is(1));
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
