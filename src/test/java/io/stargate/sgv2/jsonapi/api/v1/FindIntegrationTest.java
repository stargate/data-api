package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FindIntegrationTest extends CollectionResourceBaseIntegrationTest {

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class Find {
    @Test
    @Order(1)
    public void setUp() {
      String json =
          """
                  {
                    "insertOne": {
                      "document": {
                        "_id": "doc1",
                        "username": "user1",
                        "active_user" : true
                      }
                    }
                  }
                  """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200);

      json =
          """
                  {
                    "insertOne": {
                      "document": {
                        "_id": "doc2",
                        "username": "user2"
                      }
                    }
                  }
                  """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200);
    }

    @Test
    @Order(2)
    public void findNoFilter() {
      String json =
          """
                    {
                      "find": {
                      }
                    }
                    """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(2));
    }

    @Test
    @Order(2)
    public void findNoFilterWithOptions() {
      String json =
          """
                    {
                      "find": {
                        "options" : {
                          "pageSize" : 1
                        }
                      }
                    }
                    """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(1))
          .body("data.nextPageState", is(notNullValue()));
    }

    @Test
    @Order(2)
    public void findById() {
      String json =
          """
                    {
                      "find": {
                        "filter" : {"_id" : "doc1"}
                      }
                    }
                    """;
      String expected = "{\"_id\":\"doc1\", \"username\":\"user1\", \"active_user\":true}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void findByColumn() {
      String json =
          """
                    {
                      "find": {
                        "filter" : {"username" : "user1"}
                      }
                    }
                    """;
      String expected = "{\"_id\":\"doc1\", \"username\":\"user1\", \"active_user\":true}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void findWithEqComparisonOperator() {
      String json =
          """
          {
            "find": {
              "filter" : {"username" : {"$eq" : "user1"}}
            }
          }
          """;
      String expected = "{\"_id\":\"doc1\", \"username\":\"user1\", \"active_user\":true}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void findWithNEComparisonOperator() {
      String json =
          """
              {
                "find": {
                  "filter" : {"username" : {"$ne" : "user1"}}
                }
              }
              """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("errors[1].message", startsWith("Unsupported filter operation $ne"));
    }

    @Test
    @Order(2)
    public void findByBooleanColumn() {
      String json =
          """
                        {
                          "find": {
                            "filter" : {"active_user" : true}
                          }
                        }
                        """;
      String expected = "{\"_id\":\"doc1\", \"username\":\"user1\", \"active_user\":true}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expected));
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FindOne {
    @Test
    @Order(1)
    public void setUp() {
      String json =
          """
                 {
                   "insertOne": {
                     "document": {
                       "_id": "doc1",
                       "username": "user1",
                       "active_user" : true
                     }
                   }
                 }
                 """;
      ;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200);

      json =
          """
                  {
                    "insertOne": {
                      "document": {
                        "_id": "doc2",
                        "username": "user2"
                      }
                    }
                  }
                  """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200);
    }

    @Test
    @Order(2)
    public void findOneNoFilter() {
      String json =
          """
                    {
                      "findOne": {
                      }
                    }
                    """;
      String expected = "{\"username\": \"user1\"}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(1));
    }

    @Test
    @Order(2)
    public void findOneById() {
      String json =
          """
                    {
                      "findOne": {
                        "filter" : {"_id" : "doc1"}
                      }
                    }
                    """;
      String expected = "{\"_id\":\"doc1\", \"username\":\"user1\", \"active_user\":true}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void findOneByColumn() {
      String json =
          """
                    {
                      "findOne": {
                        "filter" : {"username" : "user1"}
                      }
                    }
                    """;
      String expected = "{\"_id\":\"doc1\", \"username\":\"user1\", \"active_user\":true}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }
  }
}
