package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsDDLSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

/**
 * Separate integration test (its own container with a deliberately small DB-wide index budget) that
 * verifies the index-limit pre-flight of {@code alterCollection}: enabling lexical adds one SAI,
 * and if that would exceed the database index limit it must be rejected with {@code
 * TOO_MANY_INDEXES_FOR_COLLECTION} before any DDL runs.
 *
 * <p>Companion to {@link CreateCollectionTooManyIndexesIntegrationTest}, which covers the same
 * limit for {@code createCollection}.
 */
@QuarkusIntegrationTest
@QuarkusTestResource(
    value = AlterCollectionTooManyIndexesIntegrationTest.LowIndexBudgetTestResource.class,
    restrictToAnnotatedClass = true)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class AlterCollectionTooManyIndexesIntegrationTest extends AbstractKeyspaceIntegrationTestBase {

  // A lexical-disabled, non-vector collection uses 9 SAIs (a lexical-enabled one uses 10 — see
  // CreateCollectionTooManyIndexesIntegrationTest). With the DB budget capped at 10, creating the
  // disabled collection fits, but enabling lexical afterwards needs the 10th index.
  private static final int INDEXES_PER_DB = 10;

  public static class LowIndexBudgetTestResource extends DseTestResource {
    public LowIndexBudgetTestResource() {}

    @Override
    public int getIndexesPerDBOverride() {
      return INDEXES_PER_DB;
    }
  }

  @Test
  public void enableLexicalRejectedWhenIndexBudgetExhausted() {
    Assumptions.assumeTrue(isLexicalAvailableForDB());

    final String coll = "alter_lex_limit";

    // 1) Create a collection with lexical disabled (uses 9 of the 10 available SAIs).
    String create =
            """
        {
          "createCollection": {
            "name": "%s",
            "options": { "lexical": { "enabled": false } }
          }
        }
        """
            .formatted(coll);
    givenHeadersAndJson(create)
        .when()
        .post(KeyspaceResource.BASE_PATH, keyspaceName)
        .then()
        .statusCode(200)
        .body("$", responseIsDDLSuccess())
        .body("status.ok", is(1));

    // 2) Push the DB to its index ceiling out-of-band (via CQL, to bypass the API's own create-time
    //    limit check). 9 (collection) + 2 (padding) = 11 SAIs, already over the limit of 10.
    boolean padded =
        executeCqlStatement(
            "CREATE TABLE \"%s\".\"alter_lex_pad\" (id int PRIMARY KEY, c0 int, c1 int)"
                .formatted(keyspaceName),
            "CREATE CUSTOM INDEX alter_lex_pad_c0 ON \"%s\".\"alter_lex_pad\" (c0) USING 'StorageAttachedIndex'"
                .formatted(keyspaceName),
            "CREATE CUSTOM INDEX alter_lex_pad_c1 ON \"%s\".\"alter_lex_pad\" (c1) USING 'StorageAttachedIndex'"
                .formatted(keyspaceName));
    assertTrue(padded, "Pre-condition: padding table and indexes should be created");

    // 3) enableLexical needs one more SAI -> over the limit -> rejected by the pre-flight, no DDL.
    String alter =
        """
        {
          "alterCollection": {
            "operation": { "enableLexical": { } }
          }
        }
        """;
    givenHeadersAndJson(alter)
        .when()
        .post(CollectionResource.BASE_PATH, keyspaceName, coll)
        .then()
        .statusCode(200)
        .body("$", responseIsError())
        .body(
            "errors[0].errorCode", is(SchemaException.Code.TOO_MANY_INDEXES_FOR_COLLECTION.name()));
  }
}
