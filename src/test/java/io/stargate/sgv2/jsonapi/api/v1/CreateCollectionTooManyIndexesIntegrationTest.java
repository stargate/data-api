package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;
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
 * Indexes being created: that is, cannot create enough indexes for a new Collection.
 */
@QuarkusIntegrationTest
@QuarkusTestResource(CreateCollectionTooManyIndexesIntegrationTest.TooManyIndexesTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class CreateCollectionTooManyIndexesIntegrationTest extends AbstractNamespaceIntegrationTestBase {
  // Number of Collections that can be created without exceeding indexes:
  private static final int COLLECTIONS_TO_CREATE = 3;
  // Defaults are changed in `StargateTestResource`, need stricter limits to trigger
  // test failure
  public static class TooManyIndexesTestResource extends DseTestResource {
    // We need 10 indexes per collection, so set to 30 to allow 3 collections
    @Override
    public int getIndexesPerDBOverride() {
      return COLLECTIONS_TO_CREATE * 10;
    }

    // But raise actual maximum collections twice the number we create so that we
    // will not hit Collection limit (but Index limit)
    @Override
    public int getMaxCollectionsPerDBOverride() {
      return COLLECTIONS_TO_CREATE * 2;
    }
  }

  @Nested
  @Order(1)
  class TooManyIndexes {
    @Test
    public void enforceMaxIndexes() {
      // Don't use auto-generated namespace that rest of the test uses
      final String NS = "ns_too_many_indexes";
      createNamespace(NS);
      final String createTemplate =
          """
              {
                "createCollection": {
                  "name": "tooManyIx_%d"
                }
              }
              """;

      // First create max collections, should work fine
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
              matchesPattern(
                  "Too many indexes: cannot create a new collection; \\d+ indexes already created in database, maximum \\d+"))
          .body("errors[0].errorCode", is("TOO_MANY_INDEXES"))
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
