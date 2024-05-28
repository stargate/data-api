package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.notNullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class SchemaCacheIntegrationTest extends AbstractNamespaceIntegrationTestBase {

  // When a table is dropped, corresponding schema cache entry should be evicted
  @Test
  @Order(1)
  public void createDropDifferentVectorDimension() {
    String json =
        """
                {
                    "createCollection": {
                        "name": "cacheTestTable",
                        "options": {
                            "vector": {
                                "metric": "cosine",
                                "dimension": 5,
                                "service": {
                                    "provider": "custom",
                                    "modelName": "text-embedding-ada-002",
                                    "authentication": {
                                        "providerKey" : "shared_creds.providerKey"
                                    },
                                    "parameters": {
                                        "projectId": "test project"
                                    }
                                }
                            }
                        }
                    }
                }
                """;
    given()
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(NamespaceResource.BASE_PATH, namespaceName)
        .then()
        .statusCode(200)
        .body("status.ok", is(1));

    // insertOne to trigger the schema cache
    json =
        """
                                          {
                                             "insertOne": {
                                                "document": {
                                                    "_id": "1",
                                                    "name": "Coded Cleats",
                                                    "description": "ChatGPT integrated sneakers that talk to you",
                                                    "$vectorize": "ChatGPT integrated sneakers that talk to you"
                                                }
                                             }
                                          }
                                          """;

    given()
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(CollectionResource.BASE_PATH, namespaceName, "cacheTestTable")
        .then()
        .statusCode(200)
        .body("status.insertedIds[0]", is("1"))
        .body("data", is(nullValue()))
        .body("errors", is(nullValue()));

    // DeleteCollection, should evict the corresponding schema cache
    given()
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(
                """
                                                {
                                                  "deleteCollection": {
                                                    "name": "%s"
                                                  }
                                                }
                                                """
                .formatted("cacheTestTable"))
        .when()
        .post(NamespaceResource.BASE_PATH, namespaceName)
        .then()
        .statusCode(200)
        .body("status.ok", is(1));

    // Create a new collection with same name, but dimension as 6
    json =
        """
                                                {
                                                    "createCollection": {
                                                        "name": "cacheTestTable",
                                                        "options": {
                                                            "vector": {
                                                                "metric": "cosine",
                                                                "dimension": 6,
                                                                "service": {
                                                                    "provider": "custom",
                                                                    "modelName": "text-embedding-ada-002",
                                                                    "authentication": {
                                                                        "providerKey" : "shared_creds.providerKey"
                                                                    },
                                                                    "parameters": {
                                                                        "projectId": "test project"
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                """;
    given()
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(NamespaceResource.BASE_PATH, namespaceName)
        .then()
        .statusCode(200)
        .body("status.ok", is(1));

    // insertOne, should use the new collectionSetting, since the outdated one has been evicted
    json =
        """
                                          {
                                             "insertOne": {
                                                "document": {
                                                    "_id": "1",
                                                    "name": "Coded Cleats",
                                                    "description": "ChatGPT integrated sneakers that talk to you",
                                                    "$vectorize": "ChatGPT integrated sneakers that talk to you"
                                                }
                                             }
                                          }
                                          """;

    given()
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(CollectionResource.BASE_PATH, namespaceName, "cacheTestTable")
        .then()
        .statusCode(200)
        .body("status.insertedIds[0]", is("1"))
        .body("data", is(nullValue()))
        .body("errors", is(nullValue()));

    // find, verify the dimension
    json =
        """
                                          {
                                            "find": {
                                              "projection": { "$vector": 1, "$vectorize" : 1 },
                                              "sort" : {"$vectorize" : "ChatGPT integrated sneakers that talk to you"}
                                            }
                                          }
                                          """;

    given()
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(CollectionResource.BASE_PATH, namespaceName, "cacheTestTable")
        .then()
        .statusCode(200)
        .body("data.documents", hasSize(1))
        .body("data.documents[0]._id", is("1"))
        .body("data.documents[0].$vector", is(notNullValue()))
        .body("data.documents[0].$vector", contains(0.1f, 0.15f, 0.3f, 0.12f, 0.05f, 0.05f))
        .body("data.documents[0].$vectorize", is(notNullValue()));
  }
}
