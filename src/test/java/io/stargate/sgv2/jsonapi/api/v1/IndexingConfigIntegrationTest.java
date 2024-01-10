package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class IndexingConfigIntegrationTest extends AbstractNamespaceIntegrationTestBase {

  private static final String denyOneIndexingCollection = "deny_one_indexing_collection";

  private static final String denyManyIndexingCollection = "deny_many_indexing_collection";

  private static final String denyAllIndexingCollection = "deny_all_indexing_collection";

  private static final String allowOneIndexingCollection = "allow_one_indexing_collection";

  private static final String allowManyIndexingCollection = "allow_many_indexing_collection";

  @Nested
  @Order(1)
  class CreateCollectionAndData {
    String insertData =
          """
              {
                "insertOne": {
                  "document": {
                    "_id": "1",
                    "name": "aaron",
                    "address": {
                      "street": "1 banana street",
                      "city": "monkey town"
                    }
                  }
                }
              }
                 """;

    @Test
    public void createCollectionAndData() {
      String createDenyOneIndexingCollection =
          """
              {
                "createCollection": {
                  "name": "%s",
                  "options" : {
                    "vector" : {
                      "size" : 5,
                      "function" : "cosine"
                    },
                    "indexing" : {
                      "deny" : ["address.city"]
                    }
                  }
                }
              }
                  """
              .formatted(denyOneIndexingCollection);
      // create collection
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(createDenyOneIndexingCollection)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
      // insert data
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(insertData)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200);

      String createDenyManyIndexingCollection =
          """
              {
                "createCollection": {
                  "name": "%s",
                  "options" : {
                    "vector" : {
                      "size" : 5,
                      "function" : "cosine"
                    },
                    "indexing" : {
                      "deny" : ["name", "address"]
                    }
                  }
                }
              }
                  """
              .formatted(denyManyIndexingCollection);
      // create collection
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(createDenyManyIndexingCollection)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
      // insert data
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(insertData)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, denyManyIndexingCollection)
          .then()
          .statusCode(200);

      String createDenyAllIndexingCollection =
          """
              {
                "createCollection": {
                  "name": "%s",
                  "options" : {
                    "vector" : {
                      "size" : 5,
                      "function" : "cosine"
                    },
                    "indexing" : {
                      "deny" : ["*"]
                    }
                  }
                }
              }
                  """
              .formatted(denyAllIndexingCollection);
      // create collection
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(createDenyAllIndexingCollection)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
      // insert data
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(insertData)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, denyAllIndexingCollection)
          .then()
          .statusCode(200);

      String createAllowOneIndexingCollection =
          """
              {
                "createCollection": {
                  "name": "%s",
                  "options" : {
                    "vector" : {
                      "size" : 5,
                      "function" : "cosine"
                    },
                    "indexing" : {
                      "allow" : ["name"]
                    }
                  }
                }
              }
                  """
              .formatted(allowOneIndexingCollection);
      // create collection
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(createAllowOneIndexingCollection)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
      // insert data
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(insertData)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, allowOneIndexingCollection)
          .then()
          .statusCode(200);

      String createAllowManyIndexingCollection =
          """
              {
                "createCollection": {
                  "name": "%s",
                  "options" : {
                    "vector" : {
                      "size" : 5,
                      "function" : "cosine"
                    },
                    "indexing" : {
                      "allow" : ["name", "address.city"]
                    }
                  }
                }
              }
                  """
              .formatted(allowManyIndexingCollection);
      // create collection
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(createAllowManyIndexingCollection)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
      // insert data
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(insertData)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, allowManyIndexingCollection)
          .then()
          .statusCode(200);
    }
  }

  @Nested
  @Order(2)
  class IndexingConfigTest {

    @Test
    public void filterFieldInDenyOne() {
      String filterData =
          """
              {
                "find": {
                  "filter": {"address.city": "monkey town"}
                }
              }
                """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterData)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors[0].message", endsWith("The filter path ('address.city') is not indexed"))
          .body("errors[0].errorCode", is("UNINDEXED_FILTER_PATH"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void filterFieldNotInDenyOne() {
      String filterData1 =
          """
              {
                "find": {
                  "filter": {"name": "aaron"}
                }
              }
                """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterData1)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(1));
      // deny "address.city", only this as a string, not "address" as an object
      String filterData2 =
              """
                  {
                    "find": {
                      "filter": {
                        "address": {
                          "$eq": {
                            "street": "1 banana street",
                            "city": "monkey town"
                          }
                        }
                      }
                    }
                  }
                    """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(filterData2)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, denyOneIndexingCollection)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void filterFieldInDenyMany() {
      // deny "address", "address.city" should also be included
      String filterData =
              """
                  {
                    "find": {
                      "filter": {"address.city": "monkey town"}
                    }
                  }
                    """;
      given()
              .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
              .contentType(ContentType.JSON)
              .body(filterData)
              .when()
              .post(CollectionResource.BASE_PATH, namespaceName, denyOneIndexingCollection)
              .then()
              .statusCode(200)
              .body("status", is(nullValue()))
              .body("data", is(nullValue()))
              .body("errors[0].message", endsWith("The filter path ('address.city') is not indexed"))
              .body("errors[0].errorCode", is("UNINDEXED_FILTER_PATH"))
              .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void filterFieldNotInDenyMany() {

    }
  }
}
