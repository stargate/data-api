package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsFindSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsWriteSuccess;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class PaginationIntegrationTest extends AbstractCollectionIntegrationTestBase {

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(1)
  class NormalFunction {
    private static final int defaultPageSize = 20;
    private static final int documentAmount = 50;
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsWriteSuccess());
    }

    @Test
    @Order(2)
    public void threePagesCheck() {
      String json =
          """
                            {
                              "find": {
                              }
                            }
                            """;

      String nextPageState =
          given()
              .headers(getHeaders())
              .contentType(ContentType.JSON)
              .body(json)
              .when()
              .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
              .then()
              .statusCode(200)
              .body("$", responseIsFindSuccess())
              .body("data.documents", hasSize(defaultPageSize))
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

      nextPageState =
          given()
              .headers(getHeaders())
              .contentType(ContentType.JSON)
              .body(json1)
              .when()
              .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
              .then()
              .statusCode(200)
              .body("$", responseIsFindSuccess())
              .body("data.documents", hasSize(defaultPageSize))
              .extract()
              .path("data.nextPageState");

      // should be fine with the empty sort clause
      String json2 =
              """
                  {
                      "find": {
                          "sort": {},
                          "options": {
                              "pageState": "%s"
                          }
                      }
                  }
                """
              .formatted(nextPageState);

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json2)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(documentAmount - 2 * defaultPageSize))
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(documentLimit))
          .body("data.nextPageState", nullValue());
    }
  }
}
