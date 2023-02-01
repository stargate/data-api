package io.stargate.sgv3.docsapi.api.v3;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.blankString;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class InsertIntegrationTest extends CollectionResourceBaseIntegrationTest {

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

      json =
          """
          {
            "find": {
              "filter" : {"_id" : "doc3"}
            }
          }
          """;
      String expected = "{\"_id\":\"doc3\", \"username\":\"user3\"}";
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
    public void insertDocumentWithNumberId() {
      String json =
          """
                        {
                          "insertOne": {
                            "document": {
                              "_id": 4,
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
          .statusCode(200)
          .body("status.insertedIds[0]", is(4));

      json =
          """
              {
                "find": {
                  "filter" : {"_id" : 4}
                }
              }
              """;
      String expected = "{\"_id\": 4, \"username\":\"user4\"}";
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
  class InsertMany {
    @Test
    public void insertDocument() {
      String json =
          """
                    {
                      "insertMany": {
                        "documents": [{
                          "_id": "doc4",
                          "username": "user4"
                        },
                        {
                          "_id": "doc5",
                          "username": "user5"
                        }]
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
          .body("status.insertedIds", contains("doc4", "doc5"));

      json =
          """
                {
                  "find": {
                    "filter" : {"_id" : "doc4"}
                  }
                }
                """;
      String expected = "{\"_id\":\"doc4\", \"username\":\"user4\"}";
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
    public void insertDocumentWithDifferentTypes() {
      String json =
          """
                        {
                          "insertMany": {
                            "documents": [{
                              "_id": "5",
                              "username": "user_id_5"
                            },
                            {
                              "_id": 5,
                              "username": "user_id_5_number"
                            }]
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
          .body("status.insertedIds", contains("5", 5));

      json =
          """
                    {
                      "find": {
                        "filter" : {"_id" : "5"}
                      }
                    }
                    """;
      String expected = "{\"_id\":\"5\", \"username\":\"user_id_5\"}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expected));

      json =
          """
                    {
                      "find": {
                        "filter" : {"_id" : 5}
                      }
                    }
                    """;
      expected = "{\"_id\":5, \"username\":\"user_id_5_number\"}";
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
    public void emptyDocument() {
      String json =
          """
                    {
                      "insertMany": {
                        "documents": [{
                        }]
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
