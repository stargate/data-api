package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class DropKeyspaceIntegrationTest extends AbstractKeyspaceIntegrationTestBase {

  @Nested
  @Order(1)
  class DropKeyspace {

    @Test
    public final void happyPath() {
      String json =
              """
          {
            "dropKeyspace": {
              "name": "%s"
            }
          }
          """
              .formatted(keyspaceName);

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      // ensure it's dropped
      json =
          """
                  {
                    "findKeyspaces": {
                    }
                  }
                  """;

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.keyspaces", not(hasItem(keyspaceName)));
    }

    @Test
    public final void withExistingCollection() {
      String keyspace = "k%s".formatted(RandomStringUtils.randomAlphanumeric(8)).toLowerCase();
      String collection = "c%s".formatted(RandomStringUtils.randomAlphanumeric(8)).toLowerCase();

      String createKeyspace =
              """
              {
                "createKeyspace": {
                  "name": "%s"
                }
              }
              """
              .formatted(keyspace);
      String createCollection =
              """
              {
                "createCollection": {
                  "name": "%s"
                }
              }
              """
              .formatted(collection);

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createKeyspace)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollection)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspace)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      String json =
              """
          {
            "dropKeyspace": {
              "name": "%s"
            }
          }
          """
              .formatted(keyspace);

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      // ensure it's dropped
      json =
          """
                  {
                    "findKeyspaces": {
                    }
                  }
                  """;

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.keyspaces", not(hasItem(keyspace)));
    }

    @Test
    public final void notExisting() {
      String json =
          """
              {
                "dropKeyspace": {
                  "name": "whatever_not_there"
                }
              }
              """;

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
    }
  }

  @Nested
  @Order(2)
  class DeprecatedDropNamespace {

    @Test
    public final void happyPath() {
      String json =
              """
          {
            "dropNamespace": {
              "name": "%s"
            }
          }
          """
              .formatted(keyspaceName);

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.ok", is(1))
          .body(
              "status.warnings",
              hasItem(
                  "This dropNamespace has been deprecated and will be removed in future releases, use dropKeyspace instead."));

      // ensure it's dropped
      json =
          """
              {
                "findKeyspaces": {
                }
              }
              """;

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.keyspaces", not(hasItem(keyspaceName)));
    }

    @Test
    public final void withExistingCollection() {
      String keyspace = "k%s".formatted(RandomStringUtils.randomAlphanumeric(8)).toLowerCase();
      String collection = "c%s".formatted(RandomStringUtils.randomAlphanumeric(8)).toLowerCase();

      String createKeyspace =
              """
              {
                "createKeyspace": {
                  "name": "%s"
                }
              }
              """
              .formatted(keyspace);
      String createCollection =
              """
              {
                "createCollection": {
                  "name": "%s"
                }
              }
              """
              .formatted(collection);

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createKeyspace)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createCollection)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspace)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      String json =
              """
          {
            "dropNamespace": {
              "name": "%s"
            }
          }
          """
              .formatted(keyspace);

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.ok", is(1))
          .body(
              "status.warnings",
              hasItem(
                  "This dropNamespace has been deprecated and will be removed in future releases, use dropKeyspace instead."));

      // ensure it's dropped
      json =
          """
              {
                "findKeyspaces": {
                }
              }
              """;

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.keyspaces", not(hasItem(keyspace)));
    }

    @Test
    public final void notExisting() {
      String json =
          """
          {
            "dropNamespace": {
              "name": "whatever_not_there"
            }
          }
          """;

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.ok", is(1))
          .body(
              "status.warnings",
              hasItem(
                  "This dropNamespace has been deprecated and will be removed in future releases, use dropKeyspace instead."));
    }
  }

  @Nested
  @Order(3)
  class Metrics {
    @Test
    public void checkMetrics() {
      DropKeyspaceIntegrationTest.super.checkMetrics("DropKeyspaceCommand");
      // We decided to keep dropNamespace metrics and logs, even it is a deprecated command
      DropKeyspaceIntegrationTest.super.checkMetrics("DropNamespaceCommand");
      DropKeyspaceIntegrationTest.super.checkDriverMetricsTenantId();
    }
  }
}
