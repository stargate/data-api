package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsDDLSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsStatusOnly;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CreateCollectionBackwardCompatibilityIntegrationTest
    extends AbstractKeyspaceIntegrationTestBase {

  @BeforeAll
  void requireLexicalSupport() {
    // Skip the whole test class if BM25/lexical is not supported by the backend, since both
    // scenarios below depend on the API defaulting to lexical/rerank enabled.
    Assumptions.assumeTrue(
        isLexicalAvailableForDB(), "Backend does not support BM25/lexical features");
  }

  /**
   * Verifies that re-issuing {@code createCollection} for a collection that was created BEFORE the
   * lexical/rerank feature existed (its CQL comment carries no {@code lexical} or {@code rerank}
   * fields at all) does NOT fail with {@code COLLECTION_EXISTS_WITH_DIFFERENT_SETTINGS} once the
   * deployment has switched to lexical/rerank-enabled-by-default.
   *
   * <p>Background: the codebase has gone through three states:
   *
   * <ol>
   *   <li>No lexical/rerank feature at all — older collections persist with no such fields.
   *   <li>Feature exists in code but disabled by config — collections persist with explicit {@code
   *       "enabled": false}.
   *   <li>Feature enabled by default — new collections persist with the feature on.
   * </ol>
   *
   * This test covers the (1) → (3) transition. Without backward-compat handling in {@link
   * io.stargate.sgv2.jsonapi.service.operation.collections.CreateCollectionOperation}, recreating a
   * state (1) collection while the deployment is in state (3) would be rejected as "settings
   * differ", even though the user is asking for the same collection. The existing options must also
   * remain unchanged after the no-op recreate.
   */
  @Test
  public final void preLexicalRerankCollection_canBeRecreatedAfterFeatureEnabled() {
    final String collectionName = "pre_lexical_rerank_collection";
    final String commentOptionsJson = "{}";
    final String expectedOptions =
        """
        {
            "lexical": {"enabled": false},
            "rerank": {"enabled": false}
        }
        """;

    // 1. simulate a legacy collection created before lexical/rerank existed (empty options)
    createCollectionViaCql(collectionName, commentOptionsJson);

    // 2. sanity-check that findCollections renders the backward-compat defaults (disabled)
    assertSingleCollection(collectionName, expectedOptions);

    // 3. recreate the same collection via the API — must succeed, not fail with
    //    COLLECTION_EXISTS_WITH_DIFFERENT_SETTINGS
    createCollectionViaApi(
            """
        {
            "createCollection": {
                "name": "%s"
            }
        }
        """
            .formatted(collectionName));

    // 4. existing settings must be preserved (no silent overwrite to enabled)
    assertSingleCollection(collectionName, expectedOptions);

    // cleanup
    deleteCollection(collectionName);
  }

  /**
   * Verifies that re-issuing {@code createCollection} for a collection that was created when the
   * lexical/rerank feature existed in code but was config-disabled at the time (its CQL comment
   * carries explicit {@code "lexical":{"enabled":false}} and {@code "rerank":{"enabled":false}})
   * does NOT fail with {@code COLLECTION_EXISTS_WITH_DIFFERENT_SETTINGS} once the deployment has
   * switched to lexical/rerank-enabled-by-default.
   *
   * <p>This is the (2) → (3) transition (see {@link
   * #preLexicalRerankCollection_canBeRecreatedAfterFeatureEnabled()} for the full state list). It
   * is distinct from (1) → (3) because the persisted comment here has the fields written out
   * explicitly with {@code enabled:false}, not omitted entirely; the backward-compat check must
   * therefore compare the persisted disabled config against the new enabled defaults using value
   * equality (not reference equality) to recognize them as backward-compatible.
   *
   * <p>The test collection also carries a non-trivial {@code indexing.allow} list to surface any
   * unrelated mismatch between the persisted comment and the recreate request payload — an
   * empty-options collection would be too weak a probe.
   */
  @Test
  public final void disabledLexicalRerankCollection_canBeRecreatedAfterFeatureEnabled() {
    final String collectionName = "lexical_rerank_feature_disabled_collection";
    final String commentOptionsJson =
        """
        {
            "indexing": {"allow": ["documentId","projectId","userId"]},
            "lexical": {"enabled": false},
            "rerank": {"enabled": false}
        }
        """;
    final String expectedOptions =
        """
        {
            "indexing": {"allow": ["documentId","projectId","userId"]},
            "lexical": {"enabled": false},
            "rerank": {"enabled": false}
        }
        """;

    // 1. simulate a collection created when lexical/rerank existed in code but was config-disabled
    createCollectionViaCql(collectionName, commentOptionsJson);

    // 2. sanity-check that findCollections returns the persisted (disabled) options
    assertSingleCollection(collectionName, expectedOptions);

    // 3. recreate via API — request includes indexing.allow to match the existing non-lexical
    //    settings; lexical/rerank are intentionally omitted so the API's enabled-by-default kicks
    //    in. Backward-compat must accept this against the persisted disabled values.
    createCollectionViaApi(
            """
        {
            "createCollection": {
                "name": "%s",
                "options": {
                    "indexing": {"allow": ["documentId","projectId","userId"]}
                }
            }
        }
        """
            .formatted(collectionName));

    // 4. existing settings must be preserved (still disabled lexical/rerank)
    assertSingleCollection(collectionName, expectedOptions);

    // cleanup
    deleteCollection(collectionName);
  }

  // ---------------------------------------------------------------------------
  // Test helpers
  // ---------------------------------------------------------------------------

  // NOTE(2025/04/17): Using raw CQL here to precisely simulate the schema state before
  // lexical/rerank options were introduced in collection comments. It would be better to use
  // non-test code to generate this, but it's embedded in the CreateCollectionOperation. Need to
  // change in the future.
  private void createCollectionViaCql(String collectionName, String collectionOptionsJson) {
    String createTable =
        """
        CREATE TABLE IF NOT EXISTS "%s"."%s" (
            key frozen<tuple<tinyint, text>> PRIMARY KEY,
            array_contains set<text>,
            array_size map<text, int>,
            doc_json text,
            exist_keys set<text>,
            query_bool_values map<text, tinyint>,
            query_dbl_values map<text, decimal>,
            query_null_values set<text>,
            query_text_values map<text, text>,
            query_timestamp_values map<text, timestamp>,
            query_vector_value vector<float, 123>,
            tx_id timeuuid
        ) WITH comment = '{"collection":{"name":"%s","schema_version":1,"options":%s}}';
        """;
    executeCqlStatement(
        SimpleStatement.newInstance(
            createTable.formatted(
                keyspaceName, collectionName, collectionName, collectionOptionsJson)));

    String[][] indexSpecs = {
      {"array_contains", "values(array_contains)"},
      {"array_size", "entries(array_size)"},
      {"exists_keys", "values(exist_keys)"},
      {"query_bool_values", "entries(query_bool_values)"},
      {"query_dbl_values", "entries(query_dbl_values)"},
      {"query_null_values", "values(query_null_values)"},
      {"query_text_values", "entries(query_text_values)"},
      {"query_timestamp_values", "entries(query_timestamp_values)"},
    };
    for (String[] spec : indexSpecs) {
      String indexCql =
          String.format(
              "CREATE CUSTOM INDEX IF NOT EXISTS %s_%s ON \"%s\".\"%s\" (%s) USING 'StorageAttachedIndex';",
              collectionName, spec[0], keyspaceName, collectionName, spec[1]);
      assertThat(executeCqlStatement(SimpleStatement.newInstance(indexCql))).isTrue();
    }
  }

  private void assertSingleCollection(String collectionName, String expectedOptionsJson) {
    givenHeadersPostJsonThenOkNoErrors(
            """
            {
              "findCollections": {
                  "options" : {
                      "explain": true
                  }
               }
            }
            """)
        .body("$", responseIsDDLSuccess())
        .body("status.collections", hasSize(1))
        .body(
            "status.collections[0]",
            jsonEquals(
                    """
                    {
                        "name": "%s",
                        "options": %s
                    }
                    """
                    .formatted(collectionName, expectedOptionsJson)));
  }

  private void createCollectionViaApi(String createCollectionPayload) {
    givenHeadersPostJsonThenOkNoErrors(createCollectionPayload)
        .body("$", responseIsStatusOnly())
        .body("status.ok", is(1));
  }
}
