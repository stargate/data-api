package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsStatusOnly;
import static org.assertj.core.api.Assertions.assertThat;
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
      // create a collection without lexical and rerank config
      // very simple collection only with name - {"createCollection": {"name": "%s"}}
      String collectionWithoutLexicalRerank =
          """
                    CREATE TABLE "%s"."%s" (
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
            "CREATE CUSTOM INDEX IF NOT EXISTS %s_array_contains ON %s.%s (values(array_contains)) USING 'StorageAttachedIndex';",
            keyspaceName, PRE_LEXICAL_RERANK_COLLECTION_NAME, PRE_LEXICAL_RERANK_COLLECTION_NAME),
        String.format(
            "CREATE CUSTOM INDEX IF NOT EXISTS %s_array_size ON %s.%s (entries(array_size)) USING 'StorageAttachedIndex';",
            keyspaceName, PRE_LEXICAL_RERANK_COLLECTION_NAME, PRE_LEXICAL_RERANK_COLLECTION_NAME),
        String.format(
            "CREATE CUSTOM INDEX IF NOT EXISTS %s_exists_keys ON %s.%s (values(exist_keys)) USING 'StorageAttachedIndex';",
            keyspaceName, PRE_LEXICAL_RERANK_COLLECTION_NAME, PRE_LEXICAL_RERANK_COLLECTION_NAME),
        String.format(
            "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_bool_values ON %s.%s (entries(query_bool_values)) USING 'StorageAttachedIndex';",
            keyspaceName, PRE_LEXICAL_RERANK_COLLECTION_NAME, PRE_LEXICAL_RERANK_COLLECTION_NAME),
        String.format(
            "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_dbl_values ON %s.%s (entries(query_dbl_values)) USING 'StorageAttachedIndex';",
            keyspaceName, PRE_LEXICAL_RERANK_COLLECTION_NAME, PRE_LEXICAL_RERANK_COLLECTION_NAME),
        String.format(
            "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_null_values ON %s.%s (values(query_null_values)) USING 'StorageAttachedIndex';",
            keyspaceName, PRE_LEXICAL_RERANK_COLLECTION_NAME, PRE_LEXICAL_RERANK_COLLECTION_NAME),
        String.format(
            "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_text_values ON %s.%s (entries(query_text_values)) USING 'StorageAttachedIndex';",
            keyspaceName, PRE_LEXICAL_RERANK_COLLECTION_NAME, PRE_LEXICAL_RERANK_COLLECTION_NAME),
        String.format(
            "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_timestamp_values ON %s.%s (entries(query_timestamp_values)) USING 'StorageAttachedIndex';",
            keyspaceName, PRE_LEXICAL_RERANK_COLLECTION_NAME, PRE_LEXICAL_RERANK_COLLECTION_NAME)
      };
      for (String indexCql : createIndexCqls) {
        assertThat(executeCqlStatement(SimpleStatement.newInstance(indexCql))).isTrue();
      }
    }

    @Test
    @Order(2)
    public final void createCollectionWithoutLexicalRerankAgain() {
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
    }
  }
}
