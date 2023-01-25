package io.stargate.sgv3.docsapi.api.v3;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.is;

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
public class FindAndUpdateIntegrationTest extends CollectionResourceBaseIntegrationTest {

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FindAndUpdate {
    @Test
    @Order(2)
    public void findById() {
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
                                "findOneAndUpdate": {
                                  "filter" : {"_id" : "doc1"},
                                  "update" : {"active_user": false}
                                }
                              }
                              """;
      String expected = "{\"_id\":\"doc1\", \"username\":\"user1\", \"active_user\":false}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expected))
          .body("status.updatedIds[0]", is("doc1"));
    }

    @Test
    @Order(2)
    public void findByColumn() {
      String json =
          """
                                {
                                  "insertOne": {
                                    "document": {
                                      "_id": "doc2",
                                      "username": "user2",
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
                                "findOneAndUpdate": {
                                  "filter" : {"username" : "user2"},
                                  "update" : {"new_col": "new_val"}
                                }
                              }
                              """;
      String expected = "{\"_id\":\"doc2\", \"username\":\"user2\", \"new_col\": \"new_val\"}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expected))
          .body("status.updatedIds[0]", is("doc2"));
      ;
    }

    @Test
    @Order(2)
    public void findByIdAndSet() {
      String json =
          """
                                {
                                  "insertOne": {
                                    "document": {
                                      "_id": "doc3",
                                      "username": "user3",
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
                                "findOneAndUpdate": {
                                  "filter" : {"_id" : "doc3"},
                                  "update" : {"$set : {"active_user": false}}
                                }
                              }
                              """;
      String expected = "{\"_id\":\"doc3\", \"username\":\"user3\", \"active_user\":false}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expected))
          .body("status.updatedIds[0]", is("doc3"));
    }

    @Test
    @Order(2)
    public void findByColumnAndSet() {
      String json =
          """
                                {
                                  "insertOne": {
                                    "document": {
                                      "_id": "doc4",
                                      "username": "user4",
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
                                "findOneAndUpdate": {
                                  "filter" : {"username" : "user4"},
                                  "update" : {"$set : {"new_col": "new_val"}}
                                }
                              }
                              """;
      String expected = "{\"_id\":\"doc4\", \"username\":\"user4\", \"new_col\": \"new_val\"}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expected))
          .body("status.updatedIds[0]", is("doc4"));
      ;
    }

    @Test
    @Order(2)
    public void findByIdAndUnset() {
      String json =
          """
                                {
                                  "insertOne": {
                                    "document": {
                                      "_id": "doc5",
                                      "username": "user5"
                                      "unset_col": "val"
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
                                "findOneAndUpdate": {
                                  "filter" : {"_id" : "doc5"},
                                  "update" : {"$unset : {"unset_col": ""}}
                                }
                              }
                                  """;
      String expected = "{\"_id\":\"doc5\", \"username\":\"user5\"}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expected))
          .body("status.updatedIds[0]", is("doc5"));
    }
  }
}
