package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.QuarkusTestResource;
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
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class CreateNamespaceIntegrationTest extends AbstractNamespaceIntegrationTestBase {

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
          "dropNamespace": {
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
  class CreateNamespace {

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
          .body("status.ok", is(1));
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
              .formatted(namespaceName);

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
          .body("status.ok", is(1));
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
    }
  }

  @Nested
  @Order(2)
  class Metrics {
    @Test
    public void checkMetrics() {
      CreateNamespaceIntegrationTest.super.checkMetrics("CreateNamespaceCommand");
      CreateNamespaceIntegrationTest.super.checkDriverMetricsTenantId();
    }
  }
}
