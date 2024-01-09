package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class FindCollectionsIntegrationTest extends AbstractNamespaceIntegrationTestBase {

  @Nested
  @Order(1)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FindCollections {

    @Test
    @Order(1)
    public void happyPath() {
      // create first
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                {
                  "createCollection": {
                    "name": "%s"
                  }
                }
                """
                  .formatted("collection1"))
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      // then find
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                  {
                    "findCollections": { }
                  }
                  """)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.collections", hasSize(greaterThanOrEqualTo(1)))
          .body("status.collections", hasItem("collection1"));
    }

    @Test
    @Order(2)
    public void happyPathWithExplain() {
      String json =
          """
              {
                "createCollection": {
                  "name": "%s",
                  "options": {
                    "vector": {
                      "dimension": 5,
                      "metric": "cosine"
                    },
                    "indexing": {
                      "deny" : ["comment"]
                    }
                  }
                }
              }
              """
              .formatted("collection2");

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      String expected1 =
          """
                  {
                    "name": "%s"
                  }
                    """
              .formatted("collection1");
      String expected2 =
          """
              {
                  "name": "%s",
                  "options": {
                    "vector": {
                      "dimension": 5,
                      "metric": "cosine"
                    },"indexing": {
                      "deny" : ["comment"]
                    }
                  }
                }
                """
              .formatted("collection2");

      json =
          """
              {
                "findCollections": {
                  "options": {
                    "explain" : true
                  }
                }
              }
              """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.collections", hasSize(2))
          .body(
              "status.collections",
              containsInAnyOrder(jsonEquals(expected1), jsonEquals(expected2)));
    }

    @Test
    @Order(3)
    public void happyPathWithMixedCase() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                                {
                                  "createCollection": {
                                    "name": "TableName"
                                  }
                                }
                                """)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      // then find
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                                {
                                  "findCollections": { }
                                }
                                """)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.collections", hasSize(greaterThanOrEqualTo(1)))
          .body("status.collections", hasItem("TableName"));
    }

    @Test
    @Order(4)
    public void emptyNamespace() {
      // create namespace first
      String namespace = "nam" + RandomStringUtils.randomNumeric(16);
      String json =
          """
          {
            "createNamespace": {
              "name": "%s"
            }
          }
          """
              .formatted(namespace);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      // then find
      json =
          """
          {
            "findCollections": {
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(NamespaceResource.BASE_PATH, namespace)
          .then()
          .statusCode(200)
          .body("status.collections", hasSize(0));

      // cleanup
      json =
          """
          {
            "dropNamespace": {
              "name": "%s"
            }
          }
          """
              .formatted(namespace);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
    }

    @Test
    @Order(5)
    /**
     * The keyspace that exists when database is created, and check if there is no collection in
     * this default keyspace.
     */
    public void checkNamespaceHasNoCollections() {
      // then find
      String json =
          """
          {
            "findCollections": {
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(NamespaceResource.BASE_PATH, "data_endpoint_auth")
          .then()
          .statusCode(200)
          .body("status.collections", hasSize(0));
    }

    @Test
    @Order(6)
    public void notExistingNamespace() {
      // then find
      String json =
          """
              {
                "findCollections": {
                }
              }
              """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(NamespaceResource.BASE_PATH, "should_not_be_there")
          .then()
          .statusCode(200)
          .body("errors[0].errorCode", is("NAMESPACE_DOES_NOT_EXIST"))
          .body(
              "errors[0].message",
              is("Unknown namespace should_not_be_there, you must create it first."));
    }

    @Test
    @Order(7)
    public void happyPathIndexingWithExplain() {
      String json =
          """
                  {
                    "createCollection": {
                      "name": "%s",
                      "options": {
                        "indexing": {
                          "deny" : ["comment"]
                        }
                      }
                    }
                  }
                  """
              .formatted("collection4");

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      String expected1 = """
      {"name":"TableName"}
      """;
      String expected2 = """
              {"name":"collection1"}
              """;
      String expected3 =
          """
      {"name":"collection2", "options": {"vector": {"dimension":5, "metric":"cosine"}, "indexing":{"deny":["comment"]}}}
      """;
      String expected4 =
          """
              {"name":"collection4", "options":{"indexing":{"deny":["comment"]}}}
              """;
      json =
          """
                  {
                    "findCollections": {
                      "options": {
                        "explain" : true
                      }
                    }
                  }
                  """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.collections", hasSize(4))
          .body(
              "status.collections",
              containsInAnyOrder(
                  jsonEquals(expected1),
                  jsonEquals(expected2),
                  jsonEquals(expected3),
                  jsonEquals(expected4)));
    }
  }

  @Nested
  @Order(2)
  class Metrics {
    @Test
    public void checkMetrics() {
      FindCollectionsIntegrationTest.super.checkMetrics("FindCollectionsCommand");
    }
  }
}
