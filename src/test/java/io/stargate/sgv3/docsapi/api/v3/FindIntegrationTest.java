package io.stargate.sgv3.docsapi.api.v3;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.common.CqlEnabledIntegrationTestBase;
import org.apache.commons.lang3.RandomStringUtils;
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
public class FindIntegrationTest extends CqlEnabledIntegrationTestBase {

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
          .body("data.docs[0]", jsonEquals(expected));
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
}
