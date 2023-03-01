package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class DeleteManyIntegrationTest extends CollectionResourceBaseIntegrationTest {
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class DeleteMany {

    private void insert(int countOfDocument) {
      for (int i = 1; i <= countOfDocument; i++) {
        String json =
            """
                        {
                          "insertOne": {
                            "document": {
                              "_id": "doc%s",
                              "username": "user%s",
                              "status": "active"
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
    public void deleteManyById() {
      insert(1);
      String json =
          """
                      {
                        "deleteMany": {
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
          .body("status.deletedCount", is(1));
    }

    @Test
    @Order(3)
    public void deleteManyByColumn() {
      insert(5);
      String json =
          """
                      {
                        "deleteMany": {
                          "filter" : {"status": "active"}
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
          .body("status.deletedCount", is(5))
          .body("status.moreData", nullValue());
    }

    @Test
    @Order(4)
    public void deleteManyNoFilter() {
      insert(20);
      String json =
          """
                      {
                        "deleteMany": {
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
          .body("status.deletedCount", is(20))
          .body("status.moreData", nullValue());
    }

    @Test
    @Order(5)
    public void deleteManyNoFilterMoreDataFlag() {
      insert(25);
      String json =
          """
                      {
                        "deleteMany": {
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
          .body("status.deletedCount", is(20))
          .body("status.moreData", is(true));
    }

    @AfterEach
    public void cleanUpData() {
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
