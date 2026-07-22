package io.stargate.sgv2.jsonapi.service.schema.collections.spec;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import java.util.*;
import org.junit.jupiter.api.Test;

/**
 * This is the base ground truth for what the CQL statements an actual instance of a super shredding
 * table should look like. This tests that we can build a CQL string to match literal CQL, and then
 * we build tests up from there.
 *
 * <p>Try to keep as literal as possible, validation of how the super shredding table is built from
 * this test.
 *
 * <p>See {@link SuperShreddingBuilder} for more details.
 */
public class SuperShreddingCQLBuilderTest extends SuperShreddingBuilderTest {

  private static final String CREATE_TABLE_ALL_OPTIONAL =
      """
            CREATE TABLE IF NOT EXISTS "keyspace"."documents" (
                "key"                     frozen<tuple<tinyint, text>>,
                "tx_id"                   timeuuid,
                "doc_json"                text,
                "exist_keys"              set<text>,
                "array_size"              map<text, int>,
                "array_contains"          set<text>,
                "query_bool_values"       map<text, tinyint>,
                "query_dbl_values"        map<text, decimal>,
                "query_text_values"       map<text, text>,
                "query_timestamp_values"  map<text, timestamp>,
                "query_null_values"       set<text>,
                "query_vector_value"      vector<float, 1024>,
                "query_lexical_value"     text,
                PRIMARY KEY ("key")
            ) WITH
                comment = '{"collection":{"name":"documents","schema_version":2}}';
            """;

  private static final String CREATE_TABLE_NO_OPTIONAL =
      """
            CREATE TABLE IF NOT EXISTS "keyspace"."documents" (
                "key"                     frozen<tuple<tinyint, text>>,
                "tx_id"                   timeuuid,
                "doc_json"                text,
                "exist_keys"              set<text>,
                "array_size"              map<text, int>,
                "array_contains"          set<text>,
                "query_bool_values"       map<text, tinyint>,
                "query_dbl_values"        map<text, decimal>,
                "query_text_values"       map<text, text>,
                "query_timestamp_values"  map<text, timestamp>,
                "query_null_values"       set<text>,
                PRIMARY KEY ("key")
            ) WITH
                comment = '{"collection":{"name":"documents","schema_version":2}}';
            """;

  private static final String CREATE_TABLE_VECTOR_ONLY =
      """
            CREATE TABLE IF NOT EXISTS "keyspace"."documents" (
                "key"                     frozen<tuple<tinyint, text>>,
                "tx_id"                   timeuuid,
                "doc_json"                text,
                "exist_keys"              set<text>,
                "array_size"              map<text, int>,
                "array_contains"          set<text>,
                "query_bool_values"       map<text, tinyint>,
                "query_dbl_values"        map<text, decimal>,
                "query_text_values"       map<text, text>,
                "query_timestamp_values"  map<text, timestamp>,
                "query_null_values"       set<text>,
                "query_vector_value"      vector<float, 1024>,
                PRIMARY KEY ("key")
            ) WITH
                comment = '{"collection":{"name":"documents","schema_version":2}}';
            """;

  private static final String CREATE_TABLE_LEXICAL_ONLY =
      """
            CREATE TABLE IF NOT EXISTS "keyspace"."documents" (
                "key"                     frozen<tuple<tinyint, text>>,
                "tx_id"                   timeuuid,
                "doc_json"                text,
                "exist_keys"              set<text>,
                "array_size"              map<text, int>,
                "array_contains"          set<text>,
                "query_bool_values"       map<text, tinyint>,
                "query_dbl_values"        map<text, decimal>,
                "query_text_values"       map<text, text>,
                "query_timestamp_values"  map<text, timestamp>,
                "query_null_values"       set<text>,
                "query_lexical_value"     text,
                PRIMARY KEY ("key")
            ) WITH
                comment = '{"collection":{"name":"documents","schema_version":2}}';
            """;

  private static final Map<String, String> REQUIRED_INDEXES =
      Map.of(
          "documents_exist_keys",
              """
                    CREATE CUSTOM INDEX IF NOT EXISTS "documents_exist_keys"
                    ON "keyspace"."documents" (values("exist_keys"))
                    USING 'StorageAttachedIndex';
                    """,
          "documents_array_size",
              """
                    CREATE CUSTOM INDEX IF NOT EXISTS "documents_array_size"
                    ON "keyspace"."documents" (entries("array_size"))
                    USING 'StorageAttachedIndex';
                    """,
          "documents_array_contains",
              """
                    CREATE CUSTOM INDEX IF NOT EXISTS "documents_array_contains"
                    ON "keyspace"."documents" (values("array_contains"))
                    USING 'StorageAttachedIndex';
                    """,
          "documents_query_bool_values",
              """
                    CREATE CUSTOM INDEX IF NOT EXISTS "documents_query_bool_values"
                    ON "keyspace"."documents" (entries("query_bool_values"))
                    USING 'StorageAttachedIndex';
                    """,
          "documents_query_dbl_values",
              """
                    CREATE CUSTOM INDEX IF NOT EXISTS "documents_query_dbl_values"
                    ON "keyspace"."documents" (entries("query_dbl_values"))
                    USING 'StorageAttachedIndex';
                    """,
          "documents_query_text_values",
              """
                    CREATE CUSTOM INDEX IF NOT EXISTS "documents_query_text_values"
                    ON "keyspace"."documents" (entries("query_text_values"))
                    USING 'StorageAttachedIndex';
                    """,
          "documents_query_timestamp_values",
              """
                    CREATE CUSTOM INDEX IF NOT EXISTS "documents_query_timestamp_values"
                    ON "keyspace"."documents" (entries("query_timestamp_values"))
                    USING 'StorageAttachedIndex';
                    """,
          "documents_query_null_values",
              """
                    CREATE CUSTOM INDEX IF NOT EXISTS "documents_query_null_values"
                    ON "keyspace"."documents" (values("query_null_values"))
                    USING 'StorageAttachedIndex';
                    """);

  private static final Map<String, String> OPTIONAL_INDEXES =
      Map.of(
          "documents_query_vector_value",
              """
                    CREATE CUSTOM INDEX IF NOT EXISTS "documents_query_vector_value"
                    ON "keyspace"."documents" ("query_vector_value")
                    USING 'StorageAttachedIndex'
                    WITH OPTIONS = { 'similarity_function' : 'cosine', 'source_model' : 'OTHER'};
                    """,
          "documents_query_lexical_value",
              """
                    CREATE CUSTOM INDEX IF NOT EXISTS "documents_query_lexical_value"
                    ON "keyspace"."documents" ("query_lexical_value")
                    USING 'StorageAttachedIndex'
                    WITH OPTIONS = { 'index_analyzer' : 'standard'};
                    """);

  private static final Map<String, String> ALL_INDEXES;

  static {
    var local = new LinkedHashMap<>(REQUIRED_INDEXES);
    local.putAll(OPTIONAL_INDEXES);
    ALL_INDEXES = Collections.unmodifiableMap(local);
  }

  public SuperShreddingCQLBuilderTest() {
    super(true, true);
    // ^^ need constant names that will match the strings in this class, want IF NOT EXIST

  }

  private List<SuperShreddingBuilder.SuperShreddingComponent<String>> asComponents(
      String tableCql, Map<String, String> indexCql) {
    var components =
        new ArrayList<SuperShreddingBuilder.SuperShreddingComponent<String>>(1 + indexCql.size());

    components.add(
        new SuperShreddingBuilder.SuperShreddingComponent<>(
            table(), SuperShreddingBuilder.SuperShreddingComponentType.TABLE, tableCql.trim()));

    for (var indexEntry : indexCql.entrySet()) {
      components.add(
          new SuperShreddingBuilder.SuperShreddingComponent<>(
              CqlIdentifier.fromInternal(indexEntry.getKey()),
              SuperShreddingBuilder.SuperShreddingComponentType.INDEX,
              indexEntry.getValue().trim()));
    }

    return components;
  }

  @Test
  public void createTableAllOptional() {

    var expectedComponents = asComponents(CREATE_TABLE_ALL_OPTIONAL, ALL_INDEXES);

    var builder = configAllOptional(SuperShreddingCQLBuilder.cql());
    var actualComponents = builder.build();

    assertComponents(
        "createTableAllOptional()",
        upcastString(expectedComponents),
        upcastString(actualComponents));
  }

  @Test
  public void createTableNoOptional() {

    var expectedComponents = asComponents(CREATE_TABLE_NO_OPTIONAL, REQUIRED_INDEXES);

    var builder = configNoOptional(SuperShreddingCQLBuilder.cql());
    var actualComponents = builder.build();

    assertComponents(
        "createTableNoOptional()",
        upcastString(expectedComponents),
        upcastString(actualComponents));
  }

  @Test
  public void createTableVectorOnly() {

    var expectedIndexes = new LinkedHashMap<>(REQUIRED_INDEXES);
    expectedIndexes.put(
        "documents_query_vector_value", OPTIONAL_INDEXES.get("documents_query_vector_value"));
    var expectedComponents = asComponents(CREATE_TABLE_VECTOR_ONLY, expectedIndexes);

    var builder = configVectorOnly(SuperShreddingCQLBuilder.cql());
    var actualComponents = builder.build();

    assertComponents(
        "createTableVectorOnly()",
        upcastString(expectedComponents),
        upcastString(actualComponents));
  }

  @Test
  public void createTableLexicalOnly() {

    var expectedIndexes = new LinkedHashMap<>(REQUIRED_INDEXES);
    expectedIndexes.put(
        "documents_query_lexical_value", OPTIONAL_INDEXES.get("documents_query_lexical_value"));
    var expectedComponents = asComponents(CREATE_TABLE_LEXICAL_ONLY, expectedIndexes);

    var builder = configLexicalOnly(SuperShreddingCQLBuilder.cql());
    var actualComponents = builder.build();

    assertComponents(
        "createTableLexicalOnly()",
        upcastString(expectedComponents),
        upcastString(actualComponents));
  }
}
