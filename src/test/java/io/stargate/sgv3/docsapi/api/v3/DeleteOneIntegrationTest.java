package io.stargate.sgv3.docsapi.api.v3;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import org.hamcrest.collection.IsCollectionWithSize;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DeleteOneIntegrationTest extends CollectionResourceIntegrationTestBase {

  @Nested
  class DeleteOne {
    @BeforeEach
    public void setUp() {
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
    }

    @Test
    public void deleteOneNoFilter() {
      String json =
          """
                          {
                            "deleteOne": {
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

    @Test
    public void findOneById() {
      String json =
          """
                          {
                            "findOne": {
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
    public void findOneByColumn() {
      String json =
          """
                          {
                            "findOne": {
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
  }
}
