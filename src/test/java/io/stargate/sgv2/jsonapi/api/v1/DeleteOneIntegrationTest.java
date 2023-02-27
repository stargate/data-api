package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
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
public class DeleteOneIntegrationTest extends CollectionResourceBaseIntegrationTest {
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class DeleteOne {
    @Test
    @Order(2)
    public void deleteOneById() {
      String json =
          """
                              {
                                "insertOne": {
                                  "document": {
                                    "_id": "doc3",
                                    "username": "user3"
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
                            "deleteOne": {
                              "filter" : {"_id" : "doc3"}
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
          .body("status.deletedCount", is(1));
    }

    @Test
    @Order(3)
    public void deleteOneByColumn() {
      String json =
          """
                              {
                                "insertOne": {
                                  "document": {
                                    "_id": "doc4",
                                    "username": "user4"
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
                            "deleteOne": {
                              "filter" : {"username" : "user4"}
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
          .body("status.deletedCount", is(1));
    }

    @Test
    @Order(4)
    public void deleteOneNoFilter() {
      String json =
          """
                              {
                                "insertOne": {
                                  "document": {
                                    "_id": "doc3",
                                    "username": "user3"
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
                        "deleteOne": {
                           "filter": {
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
          .body("status.deletedCount", is(1));
    }

    @Test
    @Order(5)
    public void deleteOneNoMatch() {
      String json =
          """
                                  {
                                    "insertOne": {
                                      "document": {
                                        "_id": "doc5",
                                        "username": "user5"
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
                            "deleteOne": {
                               "filter" : {"username" : "user12345"}
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
          .body("status.deletedCount", is(0));
    }
  }
}
