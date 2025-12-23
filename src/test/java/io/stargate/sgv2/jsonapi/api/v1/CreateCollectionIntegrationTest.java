package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;
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
@WithTestResource(value = DseTestResource.class)
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
      final String collectionName = "col" + RandomStringUtils.insecure().nextNumeric(16);
      givenHeadersPostJsonThenOk(
                  """
                      {
                        "createCollection": {
                          "name": "%s"
                        }
                      }
                      """
                  .formatted(collectionName))
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
      deleteCollection(collectionName);
    }

    @Test
    public void caseSensitive() {
      givenHeadersPostJsonThenOk(
                  """
                      {
                        "createCollection": {
                          "name": "%s"
                        }
                      }
                      """
                  .formatted("testcollection"))
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      givenHeadersPostJsonThenOk(
                  """
                      {
                        "createCollection": {
                          "name": "%s"
                        }
                      }
                      """
                  .formatted("testCollection"))
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
      deleteCollection("testcollection");
      deleteCollection("testCollection");
    }

    @Test
    public void duplicateNonVectorCollectionName() {
      // create a non vector collection
      givenHeadersPostJsonThenOk(createNonVectorCollectionJson)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // recreate the same non vector collection
      givenHeadersPostJsonThenOk(createNonVectorCollectionJson)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // create a vector collection with the same name
      givenHeadersPostJsonThenOk(createVectorCollection)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("COLLECTION_EXISTS_WITH_DIFFERENT_SETTINGS"))
          .body("errors[0].exceptionClass", is("SchemaException"))
          .body(
              "errors[0].message",
              containsString(
                  "Collection 'simple_collection' already exists but with settings different from ones passed with 'createCollection' command"));

      deleteCollection("simple_collection");
    }

    @Test
    public void duplicateVectorCollectionName() {
      // create a vector collection
      givenHeadersPostJsonThenOk(createVectorCollection)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
      // recreate the same vector collection
      givenHeadersPostJsonThenOk(createVectorCollection)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
      // create a non vector collection with the same name
      givenHeadersPostJsonThenOk(createNonVectorCollectionJson)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("COLLECTION_EXISTS_WITH_DIFFERENT_SETTINGS"))
          .body("errors[0].exceptionClass", is("SchemaException"))
          .body(
              "errors[0].message",
              containsString(
                  "Collection 'simple_collection' already exists but with settings different from ones passed with 'createCollection' command"));

      deleteCollection("simple_collection");
    }

    @Test
    public void duplicateVectorCollectionNameWithDiffSetting() {
      // create a vector collection
      givenHeadersPostJsonThenOk(createVectorCollection)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
      // create another vector collection with the same name but different size setting
      givenHeadersPostJsonThenOk(createVectorCollectionWithOtherSizeSettings)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("COLLECTION_EXISTS_WITH_DIFFERENT_SETTINGS"))
          .body("errors[0].exceptionClass", is("SchemaException"))
          .body(
              "errors[0].message",
              containsString(
                  "Collection 'simple_collection' already exists but with settings different from ones passed with 'createCollection' command"));

      // create another vector collection with the same name but different function setting
      givenHeadersPostJsonThenOk(createVectorCollectionWithOtherFunctionSettings)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("COLLECTION_EXISTS_WITH_DIFFERENT_SETTINGS"))
          .body("errors[0].exceptionClass", is("SchemaException"))
          .body(
              "errors[0].message",
              containsString(
                  "Collection 'simple_collection' already exists but with settings different from ones passed with 'createCollection' command"));

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
      givenHeadersPostJsonThenOk(createCollectionRequest)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // Also: should be idempotent so try creating again
      givenHeadersPostJsonThenOk(createCollectionRequest)
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

      givenHeadersPostJsonThenOk(createCollectionRequest)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // Also: should be idempotent so try creating again
      givenHeadersPostJsonThenOk(createCollectionRequest)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection("simple_collection_deny_indexing");
    }

    // Test to ensure single "*" accepted for "allow" or "deny" but not both
    @Test
    public void createCollectionWithIndexingStar() {
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
      deleteCollection("simple_collection_indexing_allow_star");

      // create vector collection with indexing deny option
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
      deleteCollection("simple_collection_indexing_deny_star");

      // And then check that we can't use both
      givenHeadersPostJsonThenOk(
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
      givenHeadersPostJsonThenOk(
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
      givenHeadersPostJsonThenOk(
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
      givenHeadersPostJsonThenOk(
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
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_FIELD"))
          .body("errors[0].exceptionClass", is("RequestException"))
          .body(
              "errors[0].message",
              startsWith(
                  "'createCollection' command referenced unrecognized field(s): No option \"InDex\" exists"));
    }

    @Test
    public void failWithInvalidIdConfigOption() {
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_FIELD"))
          .body("errors[0].exceptionClass", is("RequestException"))
          .body(
              "errors[0].message",
              startsWith(
                  "'createCollection' command referenced unrecognized field(s): Unrecognized field \"unknown\""));
    }

    @Test
    public void failWithInvalidIndexingConfigOption() {
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_FIELD"))
          .body("errors[0].exceptionClass", is("RequestException"))
          .body(
              "errors[0].message",
              startsWith(
                  "'createCollection' command referenced unrecognized field(s): Unrecognized field \"unknown\""));
    }

    @Test
    public void failWithInvalidVectorConfigOption() {
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_FIELD"))
          .body("errors[0].exceptionClass", is("RequestException"))
          .body(
              "errors[0].message",
              startsWith(
                  "'createCollection' command referenced unrecognized field(s): Unrecognized field \"unknown\""));
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
      givenHeadersPostJsonThenOk(createCollectionRequest)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // Also: should be idempotent so try creating again
      givenHeadersPostJsonThenOk(createCollectionRequest)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");
    }

    @Test
    public void failProviderNotSupport() {
      // create a collection with embedding service provider not support
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("SchemaException"))
          .body(
              "errors[0].message",
              startsWith(
                  "'createCollection' command option(s) invalid: Service provider 'test' is not supported"));
    }

    @Test
    public void failUnsupportedModel() {
      // create a collection with unsupported model name
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("SchemaException"))
          .body(
              "errors[0].message",
              startsWith(
                  "'createCollection' command option(s) invalid: Model name 'testModel' for provider 'azureOpenAI' is not supported"));
    }
  }

  private static Stream<Arguments> deprecatedEmbeddingModelSource() {
    return Stream.of(
        Arguments.of(
            "DEPRECATED",
            "a-deprecated-nvidia-embedding-model",
            SchemaException.Code.DEPRECATED_AI_MODEL),
        Arguments.of(
            "END_OF_LIFE",
            "a-EOL-nvidia-embedding-model",
            SchemaException.Code.END_OF_LIFE_AI_MODEL));
  }

  @ParameterizedTest
  @MethodSource("deprecatedEmbeddingModelSource")
  public void failDeprecatedEOLEmbedModel(
      String status, String modelName, SchemaException.Code errorCode) {
    givenHeadersPostJsonThenOk(
                """

                        {
                            "createCollection": {
                                "name": "bad_nvidia_model",
                                "options": {
                                    "vector": {
                                        "dimension": 1024,
                                        "service": {
                                            "provider": "nvidia",
                                            "modelName": "%s"
                                        }
                                    }
                                }
                            }
                        }
                        """
                .formatted(modelName))
        .body("$", responseIsError())
        .body(
            "errors[0].message",
            containsString("The model is: %s. It is at %s status".formatted(modelName, status)))
        .body("errors[0].errorCode", is(errorCode.name()));
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
                                              "provider": "nvidia",
                                              "modelName": "NV-Embed-QA"
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
                                          "dimension": 1024,
                                          "service": {
                                              "provider": "nvidia",
                                              "modelName": "NV-Embed-QA"
                                          }
                                      }
                                  }
                              }
                          }
                          """;
      // create vector collection with vector service and no dimension
      givenHeadersPostJsonThenOk(createCollectionWithoutDimension)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // Also: should be idempotent when try creating with correct dimension
      givenHeadersPostJsonThenOk(createCollectionWithDimension)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");

      // create vector collection with vector service and correct dimension
      givenHeadersPostJsonThenOk(createCollectionWithDimension)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // Also: should be idempotent when try creating with no dimension
      givenHeadersPostJsonThenOk(createCollectionWithoutDimension)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");
    }

    @Test
    public void failNoServiceProviderAndNoDimension() {
      // create a collection with no dimension and service
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("SchemaException"))
          .body(
              "errors[0].message",
              startsWith(
                  "'createCollection' command option(s) invalid: The 'dimension' can not be null if 'service' is not provided"));
    }

    @Test
    public void failFixDimensionUnmatchedVectorDimension() {
      // create a collection with unmatched vector dimension
      givenHeadersPostJsonThenOk(
              """
                            {
                                "createCollection": {
                                    "name": "collection_with_vector_service",
                                    "options": {
                                        "vector": {
                                            "metric": "cosine",
                                            "dimension": 123,
                                            "service": {
                                                "provider": "nvidia",
                                                "modelName": "NV-Embed-QA"
                                            }
                                        }
                                    }
                                }
                            }
                            """)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("SchemaException"))
          .body(
              "errors[0].message",
              startsWith(
                  "'createCollection' command option(s) invalid: The provided dimension value '123' doesn't match the model's supported dimension value '1024'"));
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
      givenHeadersPostJsonThenOk(createCollectionWithoutDimension)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // Also: should be idempotent when try creating with correct dimension
      givenHeadersPostJsonThenOk(createCollectionWithDefaultDimension)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");

      // create vector collection with vector service and correct dimension
      givenHeadersPostJsonThenOk(createCollectionWithDefaultDimension)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // Also: should be idempotent when try creating with no dimension
      givenHeadersPostJsonThenOk(createCollectionWithoutDimension)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");
    }

    @Test
    public void happyRangeDimensionInRange() {
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");
    }

    @Test
    public void failRangeDimensionNotInRange() {
      // create a collection with a dimension lower than the min
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("SchemaException"))
          .body(
              "errors[0].message",
              startsWith(
                  "'createCollection' command option(s) invalid: The provided dimension value (1) is not within the supported numeric range [2, 1536]"));

      // create a collection with a dimension higher than the min
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("SchemaException"))
          .body(
              "errors[0].message",
              startsWith(
                  "'createCollection' command option(s) invalid: The provided dimension value (2000) is not within the supported numeric range [2, 1536]"));
    }
  }

  @Nested
  @Order(5)
  class CreateCollectionWithEmbeddingServiceTestAuth {
    @Test
    public void happyWithNoneAuth() {
      // create a collection without providing authentication
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");
    }

    @Test
    public void failNotExistAuthKey() {
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("SchemaException"))
          .body(
              "errors[0].message",
              startsWith(
                  "'createCollection' command option(s) invalid: Service provider 'nvidia' does not support authentication key 'providerKey'"));
    }

    @Test
    public void failNoneAndHeaderDisabled() {
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("SchemaException"))
          .body(
              "errors[0].message",
              startsWith(
                  "'createCollection' command option(s) invalid: Service provider 'huggingface' does not support either 'NONE' or 'HEADER' authentication types."));
    }

    @Test
    public void failInvalidAuthKey() {
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("SchemaException"))
          .body(
              "errors[0].message",
              startsWith(
                  "'createCollection' command option(s) invalid: Service provider 'openai' does not support authentication key 'test'"));
    }

    @Test
    public void happyValidAuthKey() {
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");
    }

    @Test
    public void happyProviderKeyFormat() {
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      deleteCollection("collection_with_vector_service");

      givenHeadersPostJsonThenOk(
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
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("SchemaException"))
          .body(
              "errors[0].message",
              startsWith(
                  "'createCollection' command option(s) invalid: Required parameter 'resourceName' for the provider 'azureOpenAI' missing"));
    }

    @Test
    public void failWithUnrecognizedProviderParameters() {
      // create a collection with unrecognized parameters
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("SchemaException"))
          .body(
              "errors[0].message",
              startsWith(
                  "'createCollection' command option(s) invalid: Unexpected parameter 'test' for the provider 'azureOpenAI' provided"));
    }

    @Test
    public void failWithUnexpectedProviderParameters() {
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("SchemaException"))
          .body(
              "errors[0].message",
              startsWith(
                  "'createCollection' command option(s) invalid: Unexpected parameter 'test' for the provider 'openai' provided"));
    }

    @Test
    public void failWithWrongProviderParameterType() {
      // create a collection with wrong parameter type
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("SchemaException"))
          .body(
              "errors[0].message",
              startsWith(
                  "'createCollection' command option(s) invalid: The provided parameter 'resourceName' type is incorrect. Expected: 'string'"));
    }

    @Test
    public void failWithMissingModelParameters() {
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("SchemaException"))
          .body(
              "errors[0].message",
              startsWith(
                  "'createCollection' command option(s) invalid: Required parameter 'autoTruncate' for the provider 'vertexai' missing"));
    }

    @Test
    public void failWithUnexpectedModelParameters() {
      // create a collection with unrecognized parameters
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("SchemaException"))
          .body(
              "errors[0].message",
              startsWith(
                  "'createCollection' command option(s) invalid: Unexpected parameter 'vectorDimension' for the provider 'azureOpenAI' provided"));
    }

    @Test
    public void failWithWrongModelParameterType() {
      // create a collection with wrong parameter type
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("INVALID_CREATE_COLLECTION_OPTIONS"))
          .body("errors[0].exceptionClass", is("SchemaException"))
          .body(
              "errors[0].message",
              startsWith(
                  "'createCollection' command option(s) invalid: The provided parameter 'deploymentId' type is incorrect. Expected: 'string'"));
    }
  }

  @Nested
  @Order(7)
  class CreateCollectionWithSourceModel {
    @Test
    public void happyWithSourceModelAndMetrics() {
      // create a collection with source model and metric
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // verify the collection using FindCollection
      givenHeadersPostJsonThenOk(
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
          .body("status.collections[0].options.vector.metric", is("cosine"))
          .body("status.collections[0].options.vector.sourceModel", is("openai-v3-small"));

      deleteCollection("collection_with_sourceModel_metric");
    }

    @Test
    public void happyWithSourceModelOnly() {
      // create a collection with source model - metric will be auto-populated to 'dot_product'
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // verify the collection using FindCollection
      givenHeadersPostJsonThenOk(
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
          .body("status.collections[0].options.vector.metric", is("dot_product"))
          .body("status.collections[0].options.vector.sourceModel", is("openai-v3-small"));

      deleteCollection("collection_with_sourceModel");
    }

    @Test
    public void happyWithMetricOnly() {
      // create a collection with metric - source model will be auto-populated to 'other'
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // verify the collection using FindCollection
      givenHeadersPostJsonThenOk(
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
          .body("status.collections[0].options.vector.metric", is("cosine"))
          .body("status.collections[0].options.vector.sourceModel", is("other"));

      deleteCollection("collection_with_metric");
    }

    @Test
    public void happyNoSourceModelAndMetric() {
      // create a collection without sourceModel and metric - source model will be auto-populated to
      // 'other' and metric to 'cosine'
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // verify the collection using FindCollection
      givenHeadersPostJsonThenOk(
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
          .body("status.collections[0].options.vector.metric", is("cosine"))
          .body("status.collections[0].options.vector.sourceModel", is("other"));

      deleteCollection("collection_with_no_sourceModel_metric");
    }

    @Test
    public void failWithInvalidSourceModel() {
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("COMMAND_FIELD_VALUE_INVALID"))
          .body("errors[0].exceptionClass", is("RequestException"))
          .body(
              "errors[0].message",
              startsWith(
                  "Command field 'command.options.vector.sourceModel' value \"invalidName\" not valid"));
    }

    @Test
    public void failWithInvalidSourceModelObject() {
      givenHeadersPostJsonThenOk(
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
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("COMMAND_FIELD_VALUE_INVALID"))
          .body("errors[0].exceptionClass", is("RequestException"))
          .body(
              "errors[0].message",
              startsWith(
                  "Command field 'command.options.vector.sourceModel' value \"invalidName\" not valid:"));
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
