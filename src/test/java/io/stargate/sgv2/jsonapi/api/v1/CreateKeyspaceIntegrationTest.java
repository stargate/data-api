package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class CreateKeyspaceIntegrationTest extends AbstractKeyspaceIntegrationTestBase {

  private static final String DB_NAME = "stargate";

  @BeforeAll
  public static void enableLog() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @AfterEach
  public void deleteKeyspace() {
    String json =
            """
        {
          "dropKeyspace": {
            "name": "%s"
          }
        }
        """
            .formatted(DB_NAME);

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

  @Nested
  @Order(1)
  class CreateKeyspace {

    @Test
    public final void happyPath() {
      String json =
              """
          {
            "createKeyspace": {
              "name": "%s"
            }
          }
          """
              .formatted(DB_NAME);

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

    @Test
    public final void alreadyExists() {
      String json =
              """
          {
            "createKeyspace": {
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
    }

    @Test
    public final void withReplicationFactor() {
      String json =
              """
          {
            "createKeyspace": {
              "name": "%s",
              "options": {
                "replication": {
                  "class": "SimpleStrategy",
                  "replication_factor": 2
                }
              }
            }
          }
          """
              .formatted(DB_NAME);

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

    @Test
    public void invalidCommand() {
      String json =
          """
                      {
                        "createKeyspace": {
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
          .body("errors[0].errorCode", is("COMMAND_FIELD_INVALID"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body(
              "errors[0].message",
              is(
                  "Request invalid: field 'command.name' value `null` not valid. Problem: must not be null."));
    }
  }

  @Nested
  @Order(2)
  class DeprecatedCreateNamespace {

    @Test
    public final void happyPath() {
      String json =
              """
          {
            "createNamespace": {
              "name": "%s"
            }
          }
          """
              .formatted(DB_NAME);

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
                  "This createNamespace has been deprecated and will be removed in future releases, use createKeyspace instead."));
    }

    @Test
    public final void alreadyExists() {
      String json =
              """
          {
            "createNamespace": {
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
                  "This createNamespace has been deprecated and will be removed in future releases, use createKeyspace instead."));
    }

    @Test
    public final void withReplicationFactor() {
      String json =
              """
          {
            "createNamespace": {
              "name": "%s",
              "options": {
                "replication": {
                  "class": "SimpleStrategy",
                  "replication_factor": 2
                }
              }
            }
          }
          """
              .formatted(DB_NAME);

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
                  "This createNamespace has been deprecated and will be removed in future releases, use createKeyspace instead."));
    }

    @Test
    public void invalidCommand() {
      String json =
          """
                          {
                            "createNamespace": {
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
          .body("errors[0].errorCode", is("COMMAND_FIELD_INVALID"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body(
              "errors[0].message",
              is(
                  "Request invalid: field 'command.name' value `null` not valid. Problem: must not be null."));
      // Since command failed in Deserialization, so command result won't have deprecated command
      // warning.
    }
  }

  @Nested
  @Order(3)
  class Metrics {
    @Test
    public void checkMetrics() {
      CreateKeyspaceIntegrationTest.super.checkMetrics("CreateKeyspaceCommand");
      // We decided to keep createNamespace metrics and logs, even it is a deprecated command
      CreateKeyspaceIntegrationTest.super.checkMetrics("CreateNamespaceCommand");
      CreateKeyspaceIntegrationTest.super.checkDriverMetricsTenantId();
    }
  }
}