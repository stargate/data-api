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
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class CreateCollectionBackwardCompatibilityIntegrationTest
    extends AbstractKeyspaceIntegrationTestBase {

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

  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class CreateCollectionWithLexicalRerankBackwardCompatibility {
    private static final String PRE_LEXICAL_RERANK_COLLECTION_NAME =
        "pre_lexical_rerank_collection";

    private static final String COMMENT_OPTIONS_JSON = "{}";

    private static final String EXPECTED_OPTIONS_JSON =
        """
        {
            "lexical": {"enabled": false},
            "rerank": {"enabled": false}
        }
        """;

    @BeforeAll
    void requireLexicalSupport() {
      // Skip the whole nested class if BM25/lexical is not supported by the backend
      Assumptions.assumeTrue(
          isLexicalAvailableForDB(), "Backend does not support BM25/lexical features");
    }

    @Test
    @Order(1)
    public final void createPreLexicalRerankCollection() {
      createCollectionViaCql(PRE_LEXICAL_RERANK_COLLECTION_NAME, COMMENT_OPTIONS_JSON);

      assertSingleCollection(PRE_LEXICAL_RERANK_COLLECTION_NAME, EXPECTED_OPTIONS_JSON);
    }

    @Test
    @Order(2)
    public final void createCollectionWithoutLexicalRerankUsingAPI() {
      assertSingleCollection(PRE_LEXICAL_RERANK_COLLECTION_NAME, EXPECTED_OPTIONS_JSON);

      // create the same collection using API - should not get
      // COLLECTION_EXISTS_WITH_DIFFERENT_SETTINGS error
      createCollectionViaApi(
              """
              {
                  "createCollection": {
                      "name": "%s"
                  }
              }
              """
              .formatted(PRE_LEXICAL_RERANK_COLLECTION_NAME));

      assertSingleCollection(PRE_LEXICAL_RERANK_COLLECTION_NAME, EXPECTED_OPTIONS_JSON);

      // clean up and delete the collection
      deleteCollection(PRE_LEXICAL_RERANK_COLLECTION_NAME);
    }
  }

  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class CreateCollectionWithLexicalRerankDisabledButThenEnabledBackwardCompatibility {
    private static final String LEXICAL_RERANK_FEATURE_DISABLED_COLLECTION_NAME =
        "lexical_rerank_feature_disabled_collection";

    private static final String COMMENT_OPTIONS_JSON =
        "{\"indexing\":{\"allow\":[\"documentId\",\"projectId\",\"userId\"]}, \"lexical\":{\"enabled\":false},\"rerank\":{\"enabled\":false}}";

    private static final String EXPECTED_OPTIONS_JSON =
        """
        {
            "indexing": {"allow": ["documentId","projectId","userId"]},
            "lexical": {"enabled": false},
            "rerank": {"enabled": false}
        }
        """;

    @BeforeAll
    void requireLexicalSupport() {
      // Skip the whole nested class if BM25/lexical is not supported by the backend
      Assumptions.assumeTrue(
          isLexicalAvailableForDB(), "Backend does not support BM25/lexical features");
    }

    @Test
    @Order(1)
    public final void createLexicalRerankFeatureDisabledCollection() {
      createCollectionViaCql(LEXICAL_RERANK_FEATURE_DISABLED_COLLECTION_NAME, COMMENT_OPTIONS_JSON);

      assertSingleCollection(
          LEXICAL_RERANK_FEATURE_DISABLED_COLLECTION_NAME, EXPECTED_OPTIONS_JSON);
    }

    @Test
    @Order(2)
    public final void createCollectionWithLexicalRerankFeatureEnabledUsingAPI() {
      assertSingleCollection(
          LEXICAL_RERANK_FEATURE_DISABLED_COLLECTION_NAME, EXPECTED_OPTIONS_JSON);

      // create the same collection using API - should not get
      // COLLECTION_EXISTS_WITH_DIFFERENT_SETTINGS error
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
              .formatted(LEXICAL_RERANK_FEATURE_DISABLED_COLLECTION_NAME));

      assertSingleCollection(
          LEXICAL_RERANK_FEATURE_DISABLED_COLLECTION_NAME, EXPECTED_OPTIONS_JSON);

      // clean up and delete the collection
      deleteCollection(LEXICAL_RERANK_FEATURE_DISABLED_COLLECTION_NAME);
    }
  }
}
