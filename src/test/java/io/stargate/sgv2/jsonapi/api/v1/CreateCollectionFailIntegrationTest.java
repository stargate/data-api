package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.config.DatabaseLimitsConfig;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

/**
 * Since Collection creation failures due to maximum limits check across whole DB not just
 * Namespace, cannot isolate within main {@code CreateCollectionIntegrationTest} but need separate
 * IT class.
 */
@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class CreateCollectionFailIntegrationTest extends AbstractNamespaceIntegrationTestBase {
  @Nested
  @Order(1)
  class TooManyCollections {
    @Test
    public void enforceMaxCollections() {
      // Cannot @Inject configs into ITs so rely on constant for default values:
      final int MAX_COLLECTIONS = DatabaseLimitsConfig.DEFAULT_MAX_COLLECTIONS;
      // Don't use auto-generated namespace that rest of the test uses
      final String NS = "ns_too_many_collections";
      createNamespace(NS);
      final String createTemplate =
          """
              {
                "createCollection": {
                  "name": "tooMany_%d"
                }
              }
              """;

      // First create maximum number of collections
      for (int i = 1; i <= MAX_COLLECTIONS; ++i) {
        String json = createTemplate.formatted(i);
        given()
            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
            .contentType(ContentType.JSON)
            .body(json)
            .when()
            .post(NamespaceResource.BASE_PATH, NS)
            .then()
            .statusCode(200)
            .body("status.ok", is(1));
      }
      // And then failure
      String json = createTemplate.formatted(99);
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(NamespaceResource.BASE_PATH, NS)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              is(
                  "Too many collections: number of collections in database cannot exceed "
                      + MAX_COLLECTIONS
                      + ", already have "
                      + MAX_COLLECTIONS))
          .body("errors[0].errorCode", is("TOO_MANY_COLLECTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"));

      // But then verify that re-creating an existing one should still succeed
      // (if using same settings)
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(createTemplate.formatted(MAX_COLLECTIONS))
          .when()
          .post(NamespaceResource.BASE_PATH, NS)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
    }
  }
}
