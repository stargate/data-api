package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsDDLSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

/**
 * Separate integration test from {@code CreateCollectionIntegrationTest} to test case of too many
 * Collections per DB being created.
 */
@QuarkusIntegrationTest
@WithTestResource(
    value = CreateCollectionTooManyTablesIntegrationTest.TooManyTablesTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class CreateCollectionTooManyTablesIntegrationTest extends AbstractKeyspaceIntegrationTestBase {
  // Let's use relatively low limit to trigger test failure
  private static final int COLLECTIONS_TO_CREATE = 3;

  // Need to limit max-collections to low value, but max-indexes higher to ensure
  // we hit former
  public static class TooManyTablesTestResource extends DseTestResource {
    @Override
    public int getMaxCollectionsPerDBOverride() {
      return COLLECTIONS_TO_CREATE;
    }

    // As per requiring up to 10 collections, will also then need 100 SAIs
    @Override
    public int getIndexesPerDBOverride() {
      return COLLECTIONS_TO_CREATE * 20;
    }
  }

  @Test
  public void enforceMaxCollections() {
    // Don't use auto-generated namespace that rest of the test uses
    final String NS = "ns_too_many_collections";
    createKeyspace(NS);
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
      givenHeadersAndJson(createTemplate.formatted(i))
          .when()
          .post(KeyspaceResource.BASE_PATH, NS)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
    }
    // And then failure
    givenHeadersAndJson(createTemplate.formatted(99))
        .when()
        .post(KeyspaceResource.BASE_PATH, NS)
        .then()
        .statusCode(200)
        .body("$", responseIsError())
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
    givenHeadersAndJson(createTemplate.formatted(1))
        .when()
        .post(KeyspaceResource.BASE_PATH, NS)
        .then()
        .statusCode(200)
        .body("$", responseIsDDLSuccess())
        .body("status.ok", is(1));
  }
}
