package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class CreateCollectionIntegrationTest extends AbstractNamespaceIntegrationTestBase {
  @Nested
  @Order(1)
  class CreateCollection {
    String deleteCollectionJson =
        """
            {
              "deleteCollection": {
                "name": "simple_collection"
              }
            }
            """;
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

    String createCollectionWithAllowIndexing =
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
                        "allow" : ["field1", "field2", "address.city", "_id"]
                      }
                    }
                  }
                }
                """;

    String createCollectionWithDenyIndexing =
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

    String createCollectionWithIndexingError =
        """
                {
                  "createCollection": {
                    "name": "simple_collection_error1",
                    "options" : {
                      "vector" : {
                        "size" : 5,
                        "function" : "cosine"
                      },
                      "indexing" : {
                        "allow" : ["field1", "field1"]
                      }
                    }
                  }
                }
                """;

    String createCollectionWithIndexingError2 =
        """
                {
                  "createCollection": {
                    "name": "simple_collection_error2",
                    "options" : {
                      "vector" : {
                        "size" : 5,
                        "function" : "cosine"
                      },
                      "indexing" : {
                        "allow" : ["field1", "field2"],
                        "deny" : ["field1", "field2"]
                      }
                    }
                  }
                }
                """;

    @Test
    public void happyPath() {
      String json =
          """
          {
            "createCollection": {
              "name": "%s"
            }
          }
          """
              .formatted("col" + RandomStringUtils.randomNumeric(16));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
    }

    @Test
    public void duplicateNonVectorCollectionName() {
      // create a non vector collection
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(createNonVectorCollectionJson)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      // recreate the same non vector collection
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(createNonVectorCollectionJson)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      // create a vector collection with the same name
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(createVectorCollection)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              is(
                  "The provided collection name 'simple_collection' already exists with a non-vector setting."))
          .body("errors[0].errorCode", is("INVALID_COLLECTION_NAME"))
          .body("errors[0].exceptionClass", is("JsonApiException"));

      // delete the collection
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(deleteCollectionJson.formatted("simple_collection"))
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
    }

    @Test
    public void duplicateVectorCollectionName() {
      // create a vector collection
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(createVectorCollection)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
      // recreate the same vector collection
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(createVectorCollection)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
      // create a non vector collection with the same name
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(createNonVectorCollectionJson)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              is(
                  "The provided collection name 'simple_collection' already exists with a vector setting."))
          .body("errors[0].errorCode", is("INVALID_COLLECTION_NAME"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
      // delete the collection
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(deleteCollectionJson.formatted("simple_collection"))
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
    }

    @Test
    public void duplicateVectorCollectionNameWithDiffSetting() {
      // create a vector collection
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(createVectorCollection)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
      // create another vector collection with the same name but different size setting
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
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
              is(
                  "The provided collection name 'simple_collection' already exists with a different vector setting."))
          .body("errors[0].errorCode", is("INVALID_COLLECTION_NAME"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
      // create another vector collection with the same name but different function setting
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
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
              is(
                  "The provided collection name 'simple_collection' already exists with a different vector setting."))
          .body("errors[0].errorCode", is("INVALID_COLLECTION_NAME"))
          .body("errors[0].exceptionClass", is("JsonApiException"));

      // create vector collection with indexing allow option
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(createCollectionWithAllowIndexing)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      // create vector collection with indexing deny option
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(createCollectionWithDenyIndexing)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      // create vector collection with error indexing option
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(createCollectionWithIndexingError)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              is("Invalid indexing definition - `allow` cannot contain duplicates"))
          .body("errors[0].errorCode", is("INVALID_INDEXING_USAGE"))
          .body("errors[0].exceptionClass", is("JsonApiException"));

      // create vector collection with error indexing option
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(createCollectionWithIndexingError2)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              is("Invalid indexing definition - `allow` and `deny` cannot be used together"))
          .body("errors[0].errorCode", is("INVALID_INDEXING_USAGE"))
          .body("errors[0].exceptionClass", is("JsonApiException"));

      // delete the collection
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(deleteCollectionJson.formatted("simple_collection"))
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
    }
  }

  @Nested
  @Order(99)
  class Metrics {
    @Test
    public void checkMetrics() {
      CreateCollectionIntegrationTest.super.checkMetrics("CreateCollectionCommand");
    }
  }
}
