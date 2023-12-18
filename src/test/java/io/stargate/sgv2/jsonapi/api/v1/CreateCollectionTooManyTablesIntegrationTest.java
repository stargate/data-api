package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
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

/**
 * Separate integration test from {@code CreateCollectionIntegrationTest} to test case of too many
 * Collections per DB being created.
 */
@QuarkusIntegrationTest
@QuarkusTestResource(CreateCollectionTooManyTablesIntegrationTest.TooManyTablesTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class CreateCollectionTooManyTablesIntegrationTest extends AbstractNamespaceIntegrationTestBase {
  // Let's limit maximum number to lower than defaults
  private static final int COLLECTIONS_TO_CREATE = 5;

  // Defaults are changed in `StargateTestResource` (to allow more tables during testing),
  // need override to reset back to defaults:
  public static class TooManyTablesTestResource extends DseTestResource {
    @Override
    public int getMaxCollectionsPerDBOverride() {
      return COLLECTIONS_TO_CREATE;
    }

    // Use looser max for indexes, to avoid triggering that limit (typically
    // we need up to 10 indexes per table)
    @Override
    public int getIndexesPerDBOverride() {
      return COLLECTIONS_TO_CREATE * 12;
    }
  }

  @Nested
  @Order(1)
  class TooManyCollections {
    @Test
    public void enforceMaxCollections() {
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
      for (int i = 1; i <= COLLECTIONS_TO_CREATE; ++i) {
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
                      + COLLECTIONS_TO_CREATE
                      + ", already have "
                      + COLLECTIONS_TO_CREATE))
          .body("errors[0].errorCode", is("TOO_MANY_COLLECTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"));

      // But then verify that re-creating an existing one should still succeed
      // (if using same settings)
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(createTemplate.formatted(1))
          .when()
          .post(NamespaceResource.BASE_PATH, NS)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
    }
  }
}
