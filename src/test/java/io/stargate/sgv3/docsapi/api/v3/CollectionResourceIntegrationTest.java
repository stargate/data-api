package io.stargate.sgv3.docsapi.api.v3;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.blankString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.common.CqlEnabledIntegrationTestBase;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CollectionResourceIntegrationTest extends CqlEnabledIntegrationTestBase {
  private String collectionName = "col" + RandomStringUtils.randomNumeric(16);

  @BeforeAll
  public static void enableLog() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @Nested
  class FindOne {

    @BeforeEach
    public void setUp() {
      String json =
          """
              {
                "insertOne": {
                  "document": {
                    "_id": "doc1",
                    "username": "user1"
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
    public void findOneNoFilter() {
      String json =
          """
                {
                  "findOne": {
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
          .body("data.count", is(1));
    }

    @Test
    public void findOneById() {
      String json =
          """
                {
                  "findOne": {
                    "filter" : {"_id" : "doc1"}
                  }
                }
                """;
      String expected = "{\"_id\":\"doc1\", \"username\": \"user1\"}";
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
    public void findOneByColumn() {
      String json =
          """
                {
                  "findOne": {
                    "filter" : {"username" : "user1"}
                  }
                }
                """;
      String expected = "{\"_id\":\"doc1\", \"username\": \"user1\"}";
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
  class InsertOne {
    @Test
    public void insertDocument() {
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
          .statusCode(200)
          .body("status.insertedIds[0]", is("doc3"));
    }

    @Test
    public void emptyDocument() {
      String json =
          """
          {
            "insertOne": {
              "document": {
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
    public void notValidDocumentMissing() {
      String json =
          """
          {
            "insertOne": {
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
          .body("errors[0].message", is(not(blankString())))
          .body("errors[0].exceptionClass", is("ConstraintViolationException"));
    }
  }

  @Nested
  class ClientErrors {

    @Test
    public void tokenMissing() {
      given()
          .contentType(ContentType.JSON)
          .body("{}")
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body(
              "errors[0].message",
              is(
                  "Role unauthorized for operation: Missing token, expecting one in the X-Cassandra-Token header."));
    }

    @Test
    public void malformedBody() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body("{wrong}")
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("errors[0].message", is(not(blankString())))
          .body("errors[0].exceptionClass", is("WebApplicationException"))
          .body("errors[1].message", is(not(blankString())))
          .body("errors[1].exceptionClass", is("JsonParseException"));
    }

    @Test
    public void emptyBody() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("errors[0].message", is(not(blankString())))
          .body("errors[0].exceptionClass", is("ConstraintViolationException"));
    }
  }
}
