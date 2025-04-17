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
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class CreateCollectionBackwardCompatibilityIntegrationTest
    extends AbstractKeyspaceIntegrationTestBase {

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class CreateCollectionWithLexicalRerankBackwardCompatibility {
    private static final String PRE_LEXICAL_RERANK_COLLECTION_NAME =
        "pre_lexical_rerank_collection";

    @Test
    @Order(1)
    public final void createPreLexicalRerankCollection() {
      // NOTE(2025/04/17): Using raw CQL here to precisely simulate the schema state before
      // lexical/rerank options were introduced in collection comments. It would be better to use
      // non-test code to generate this, but it's embedded in the CreateCollectionOperation. Need to
      // change in the future
      String collectionWithoutLexicalRerank =
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
                    ) WITH comment = '{"collection":{"name":"%s","schema_version":1,"options":{"defaultId":{"type":""}}}}';
                    """;
      executeCqlStatement(
          SimpleStatement.newInstance(
              collectionWithoutLexicalRerank.formatted(
                  keyspaceName,
                  PRE_LEXICAL_RERANK_COLLECTION_NAME,
                  PRE_LEXICAL_RERANK_COLLECTION_NAME)));

      // create indexes for the collection
      String[] createIndexCqls = {
        String.format(
            "CREATE CUSTOM INDEX IF NOT EXISTS %s_array_contains ON \"%s\".\"%s\" (values(array_contains)) USING 'StorageAttachedIndex';",
            PRE_LEXICAL_RERANK_COLLECTION_NAME, keyspaceName, PRE_LEXICAL_RERANK_COLLECTION_NAME),
        String.format(
            "CREATE CUSTOM INDEX IF NOT EXISTS %s_array_size ON \"%s\".\"%s\" (entries(array_size)) USING 'StorageAttachedIndex';",
            PRE_LEXICAL_RERANK_COLLECTION_NAME, keyspaceName, PRE_LEXICAL_RERANK_COLLECTION_NAME),
        String.format(
            "CREATE CUSTOM INDEX IF NOT EXISTS %s_exists_keys ON \"%s\".\"%s\" (values(exist_keys)) USING 'StorageAttachedIndex';",
            PRE_LEXICAL_RERANK_COLLECTION_NAME, keyspaceName, PRE_LEXICAL_RERANK_COLLECTION_NAME),
        String.format(
            "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_bool_values ON \"%s\".\"%s\" (entries(query_bool_values)) USING 'StorageAttachedIndex';",
            PRE_LEXICAL_RERANK_COLLECTION_NAME, keyspaceName, PRE_LEXICAL_RERANK_COLLECTION_NAME),
        String.format(
            "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_dbl_values ON \"%s\".\"%s\" (entries(query_dbl_values)) USING 'StorageAttachedIndex';",
            PRE_LEXICAL_RERANK_COLLECTION_NAME, keyspaceName, PRE_LEXICAL_RERANK_COLLECTION_NAME),
        String.format(
            "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_null_values ON \"%s\".\"%s\" (values(query_null_values)) USING 'StorageAttachedIndex';",
            PRE_LEXICAL_RERANK_COLLECTION_NAME, keyspaceName, PRE_LEXICAL_RERANK_COLLECTION_NAME),
        String.format(
            "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_text_values ON \"%s\".\"%s\" (entries(query_text_values)) USING 'StorageAttachedIndex';",
            PRE_LEXICAL_RERANK_COLLECTION_NAME, keyspaceName, PRE_LEXICAL_RERANK_COLLECTION_NAME),
        String.format(
            "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_timestamp_values ON \"%s\".\"%s\" (entries(query_timestamp_values)) USING 'StorageAttachedIndex';",
            PRE_LEXICAL_RERANK_COLLECTION_NAME, keyspaceName, PRE_LEXICAL_RERANK_COLLECTION_NAME)
      };
      for (String indexCql : createIndexCqls) {
        assertThat(executeCqlStatement(SimpleStatement.newInstance(indexCql))).isTrue();
      }

      // verify the collection using FindCollection
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
                              "options": {
                                  "lexical": {
                                      "enabled": false
                                  },
                                  "rerank": {
                                      "enabled": false
                                  }
                              }
                          }
                      """
                      .formatted(PRE_LEXICAL_RERANK_COLLECTION_NAME)));
    }

    @Test
    @Order(2)
    @Disabled
    public final void createCollectionWithoutLexicalRerankUsingAPI() {
      // verify the preexisting collection（generated by the above CQL) using FindCollection
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
                                      "options": {
                                          "lexical": {
                                              "enabled": false
                                          },
                                          "rerank": {
                                              "enabled": false
                                          }
                                      }
                                  }
                              """
                      .formatted(PRE_LEXICAL_RERANK_COLLECTION_NAME)));

      // create the same collection using API - should not have
      // EXISTING_COLLECTION_DIFFERENT_SETTINGS error
      givenHeadersPostJsonThenOkNoErrors(
                  """
                      {
                          "createCollection": {
                              "name": "%s"
                          }
                      }
              """
                  .formatted(PRE_LEXICAL_RERANK_COLLECTION_NAME))
          .body("$", responseIsStatusOnly())
          .body("status.ok", is(1));

      // verify the collection using FindCollection again
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
                                      "options": {
                                          "lexical": {
                                              "enabled": false
                                          },
                                          "rerank": {
                                              "enabled": false
                                          }
                                      }
                                  }
                              """
                      .formatted(PRE_LEXICAL_RERANK_COLLECTION_NAME)));
    }
  }
}
