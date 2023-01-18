package io.stargate.sgv3.docsapi.api.v3;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.common.CqlEnabledIntegrationTestBase;
import org.apache.commons.lang3.RandomStringUtils;
import org.hamcrest.collection.IsCollectionWithSize;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DeleteOneIntegrationTest extends CqlEnabledIntegrationTestBase {

  protected String collectionName = "col" + RandomStringUtils.randomNumeric(16);

  @BeforeAll
  public static void enableLog() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @Test
  @Order(1)
  public final void createCollection() {
    String json =
        String.format(
            """
                                {
                                  "createCollection": {
                                    "name": "%s"
                                  }
                                }
                                """,
            collectionName);
    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(DatabaseResource.BASE_PATH, keyspaceId.asInternal())
        .then()
        .statusCode(200);
  }

  @Nested
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
          .body("status.deletedIds", is(IsCollectionWithSize.hasSize(1)))
          .body("status.deletedIds", contains("doc3"));
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
          .body("status.deletedIds", is(IsCollectionWithSize.hasSize(1)))
          .body("status.deletedIds", contains("doc4"));
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
          .body("status.deletedIds", is(IsCollectionWithSize.hasSize(1)));
    }
  }
}
