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
public class PaginationIntegrationTest extends AbstractCollectionIntegrationTestBase {

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(1)
  class NormalFunction {
    private static final int defaultPageSize = 20;
    private static final int documentAmount = 30;
    private static final int documentLimit = 5;

    @Test
    @Order(1)
    public void setUp() {
      for (int i = 0; i < documentAmount; i++) {
        insert(
            """
                              {
                                "insertOne": {
                                  "document": {
                                    "username": "testUser %s"
                                  }
                                }
                              }
                            """
                .formatted(i));
      }
    }

    private void insert(String json) {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200);
    }

    @Test
    @Order(2)
    public void twoPagesCheck() {
      String json =
          """
                            {
                              "find": {
                              }
                            }
                            """;

      String nextPageState =
          given()
              .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
              .contentType(ContentType.JSON)
              .body(json)
              .when()
              .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
              .then()
              .statusCode(200)
              .body("data.documents", hasSize(defaultPageSize))
              .body("status", is(nullValue()))
              .body("errors", is(nullValue()))
              .extract()
              .path("data.nextPageState");

      String json1 =
          """
                            {
                              "find": {
                                        "options":{
                                                  "pageState" : "%s"
                                              }
                              }
                            }
                            """
              .formatted(nextPageState);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json1)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(documentAmount - defaultPageSize))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.nextPageState", nullValue());
    }

    @Test
    @Order(3)
    public void pageLimitCheck() {
      String json =
          """
                            {
                              "find": {
                                        "options": {
                                                  "limit": %s
                                              }
                              }
                            }
                            """
              .formatted(documentLimit);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(documentLimit))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.nextPageState", nullValue());
    }
  }
}
