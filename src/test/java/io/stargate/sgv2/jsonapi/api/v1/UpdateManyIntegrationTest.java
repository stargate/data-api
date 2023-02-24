package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

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
public class UpdateManyIntegrationTest extends CollectionResourceBaseIntegrationTest {
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class UpdateMany {

    private void insert(int countOfDocument) {
      for (int i = 1; i <= countOfDocument; i++) {
        String json =
            """
                                    {
                                      "insertOne": {
                                        "document": {
                                          "_id": "doc%s",
                                          "username": "user%s",
                                          "active_user" : true
                                        }
                                      }
                                    }
                                    """;
        given()
            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
            .contentType(ContentType.JSON)
            .body(json.formatted(i, i))
            .when()
            .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
            .then()
            .statusCode(200);
      }
    }

    @Test
    @Order(2)
    public void updateManyById() {
      insert(1);
      String json =
          """
                                {
                                  "updateMany": {
                                    "filter" : {"_id" : "doc1"},
                                    "update" : {"$set" : {"active_user": false}}
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
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("status.moreData", nullValue());

      String expected = "{\"_id\":\"doc1\", \"username\":\"user1\", \"active_user\":false}";
      json =
          """
                        {
                          "find": {
                            "filter" : {"_id" : "doc1"}
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
          .body("data.docs[0]", jsonEquals(expected));

      cleanUpData();
    }

    @Test
    @Order(3)
    public void updateManyByColumn() {
      insert(5);
      String json =
          """
                                {
                                  "updateMany": {
                                    "filter" : {"active_user": true},
                                    "update" : {"$set" : {"active_user": false}}
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
          .body("status.matchedCount", is(5))
          .body("status.modifiedCount", is(5))
          .body("status.moreData", nullValue());

      String expected = "{\"_id\":\"doc1\", \"username\":\"user1\", \"active_user\":false}";
      json =
          """
                        {
                          "find": {
                            "filter" : {"_id" : "doc1"}
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
          .body("data.docs[0]", jsonEquals(expected));
      cleanUpData();
    }

    @Test
    @Order(4)
    public void updateManyLimit() {
      insert(20);
      String json =
          """
                                {
                                  "updateMany": {
                                    "filter" : {"active_user": true},
                                    "update" : {"$set" : {"active_user": false}}
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
          .body("status.matchedCount", is(20))
          .body("status.modifiedCount", is(20))
          .body("status.moreData", nullValue());

      json =
          """
                        {
                          "find": {
                            "filter" : {"active_user": false}
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
          .body("data.count", is(20));

      cleanUpData();
    }

    @Test
    @Order(5)
    public void deleteManyLimitMoreDataFlag() {
      insert(25);
      String json =
          """
                                {
                                  "updateMany": {
                                    "filter" : {"active_user" : true},
                                    "update" : {"$set" : {"active_user": false}}
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
          .body("status.matchedCount", is(20))
          .body("status.modifiedCount", is(20))
          .body("status.moreData", is(true));

      json =
          """
                        {
                          "find": {
                            "filter" : {"active_user": true}
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
          .body("data.count", is(5));
      cleanUpData();
    }

    @Test
    @Order(6)
    public void updateManyUpsert() {
      insert(5);
      String json =
          """
                                    {
                                      "updateMany": {
                                        "filter" : {"_id": "doc6"},
                                        "update" : {"$set" : {"active_user": false}},
                                        "options" : {"upsert" : true}
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
          .body("status.upsertedId", is("doc6"))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("status.moreData", nullValue());

      String expected = "{\"_id\":\"doc6\", \"active_user\":false}";
      json =
          """
                            {
                              "find": {
                                "filter" : {"_id" : "doc6"}
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
          .body("data.docs[0]", jsonEquals(expected));
      cleanUpData();
    }

    private void cleanUpData() {
      String json =
          """
                                {
                                  "deleteMany": {
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
  }
}
