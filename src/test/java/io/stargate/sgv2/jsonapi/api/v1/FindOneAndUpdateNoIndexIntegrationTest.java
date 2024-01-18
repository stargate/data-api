package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindOneAndUpdateNoIndexIntegrationTest extends AbstractNamespaceIntegrationTestBase {
  private static final String collectionName = "no_index_collection";

  @Nested
  @Order(1)
  class CreateCollection {
    @Test
    public void createBaseCollection() {
      String json =
          """
                 {
                   "createCollection": {
                     "name": "%s",
                     "options": {
                       "vector": {
                         "size": 2,
                         "function": "cosine"
                       },
                       "indexing" : {
                         "allow" : ["_id", "name", "value"]
                       }
                     }
                   }
                 }
                  """
              .formatted(collectionName);
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
    }
  }

  @Nested
  @Order(2)
  class FindAndUpdateWithSet {
    @Test
    public void byIdAfterUpdate() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                      {
                        "insertOne": {
                          "document": {
                            "_id": "update_doc_after",
                            "name": "Joe",
                            "age": 42,
                            "enabled": false,
                            "$vector" : [ 0.5, -0.25 ]
                          }
                        }
                      }
                      """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
              {
                "findOneAndUpdate": {
                  "filter" : {"_id" : "update_doc_after"},
                  "update" : {
                    "$set" : {
                      "enabled": true,
                      "value": -1
                    }
                  },
                  "options": {
                    "returnDocument" : "after"
                  }
                }
              }
              """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body(
              "data.document",
              jsonEquals(
                  """
                      {
                        "_id": "update_doc_after",
                        "name": "Joe",
                        "age": 42,
                        "enabled": true,
                        "value": -1,
                        "$vector" : [ 0.5, -0.25 ]
                      }
                      """))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));
    }

    @Test
    public void byIdBeforeUpdate() {
      final String DOC =
          """
              {
              "_id": "update_doc_before",
                "name": "Bob",
                "age": 77,
                "enabled": true,
                "$vector" : [ 0.5, -0.25 ],
                "value": 3
              }
              """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                              {
                                "insertOne": {
                                  "document": %s
                                }
                              }
                              """
                  .formatted(DOC))
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                      {
                        "findOneAndUpdate": {
                          "filter" : {"_id" : "update_doc_before"},
                          "update" : {
                            "$set" : {
                              "enabled": false,
                              "value": 4
                            }
                          },
                          "options": {
                            "returnDocument": "before"
                          }
                        }
                      }
                      """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", jsonEquals(DOC))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));
    }
  }
}
