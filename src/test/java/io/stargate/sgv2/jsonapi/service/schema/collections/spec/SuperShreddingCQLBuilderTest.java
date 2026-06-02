package io.stargate.sgv2.jsonapi.service.schema.collections.spec;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This is the base ground truth for what the CQL statements an actual instance of a
 * super shredding table should look like. This tests that we can build a CQL string
 * to match literal CQL, and then we build tests up from there.
 * <p>
 *  Try to keep as literal as possible, validation of how the super shredding table is built
 *  builds from this test.
 * </p>
 * <p>
 * See {@link SuperShreddingBuilder} for more details.
 * </p>
 */
public class SuperShreddingCQLBuilderTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SuperShreddingCQLBuilderTest.class);

    private static final CqlIdentifier KEYSPACE = CqlIdentifier.fromInternal("keyspace");
    private static final CqlIdentifier TABLE = CqlIdentifier.fromInternal("documents");
    private static final String COMMENT = """
            {"collection":{"name":"documents","schema_version":2}}""";

    private static  final String CREATE_TABLE_ALL_OPTIONAL = """
            CREATE TABLE IF NOT EXISTS "keyspace"."documents" (
                key tuple<tinyint, text> PRIMARY KEY,
                tx_id timeuuid,
                doc_json text,
                exist_keys set<text>,
                array_size map<text, int>,
                array_contains set<text>,
                query_bool_values map<text, tinyint>,
                query_dbl_values map<text, decimal>,
                query_text_values map<text, text>,
                query_timestamp_values map<text, timestamp>,
                query_null_values set<text>,
                query_vector_value vector<float, 1024>,
                query_lexical_value text,
            ) WITH
                comment = '{"collection":{"name":"documents","schema_version":2}}';
            """;

    private static  final String CREATE_TABLE_NO_OPTIONAL = """
            CREATE TABLE IF NOT EXISTS "keyspace"."documents" (
                key tuple<tinyint, text> PRIMARY KEY,
                tx_id timeuuid,
                doc_json text,
                exist_keys set<text>,
                array_size map<text, int>,
                array_contains set<text>,
                query_bool_values map<text, tinyint>,
                query_dbl_values map<text, decimal>,
                query_text_values map<text, text>,
                query_timestamp_values map<text, timestamp>,
                query_null_values set<text>,
            ) WITH
                comment = '{"collection":{"name":"documents","schema_version":2}}';
            """;

    private static final String CREATE_TABLE_VECTOR_ONLY = """
            CREATE TABLE IF NOT EXISTS "keyspace"."documents" (
                key tuple<tinyint, text> PRIMARY KEY,
                tx_id timeuuid,
                doc_json text,
                exist_keys set<text>,
                array_size map<text, int>,
                array_contains set<text>,
                query_bool_values map<text, tinyint>,
                query_dbl_values map<text, decimal>,
                query_text_values map<text, text>,
                query_timestamp_values map<text, timestamp>,
                query_null_values set<text>,
                query_vector_value vector<float, 1024>,
            ) WITH
                comment = '{"collection":{"name":"documents","schema_version":2}}';
            """;

    private static  final String CREATE_TABLE_LEXICAL_ONLY = """
            CREATE TABLE IF NOT EXISTS "keyspace"."documents" (
                key tuple<tinyint, text> PRIMARY KEY,
                tx_id timeuuid,
                doc_json text,
                exist_keys set<text>,
                array_size map<text, int>,
                array_contains set<text>,
                query_bool_values map<text, tinyint>,
                query_dbl_values map<text, decimal>,
                query_text_values map<text, text>,
                query_timestamp_values map<text, timestamp>,
                query_null_values set<text>,
                query_lexical_value text,
            ) WITH
                comment = '{"collection":{"name":"documents","schema_version":2}}';
            """;

    private static final Map<String, String> REQUIRED_INDEXES = Map.of(
            "documents_exist_keys", """
                    CREATE CUSTOM INDEX IF NOT EXISTS "documents_exist_keys"
                    ON "keyspace"."documents" (values(exist_keys))
                    USING 'StorageAttachedIndex';
                    """,
            "documents_array_size", """
                    CREATE CUSTOM INDEX IF NOT EXISTS "documents_array_size"
                    ON "keyspace"."documents" (entries(array_size))
                    USING 'StorageAttachedIndex';
                    """,
            "documents_array_contains", """
                    CREATE CUSTOM INDEX IF NOT EXISTS "documents_array_contains"
                    ON "keyspace"."documents" (values(array_contains))
                    USING 'StorageAttachedIndex';
                    """,
            "documents_query_bool_values", """
                    CREATE CUSTOM INDEX IF NOT EXISTS "documents_query_bool_values"
                    ON "keyspace"."documents" (entries(query_bool_values))
                    USING 'StorageAttachedIndex';
                    """,
            "documents_query_dbl_values", """
                    CREATE CUSTOM INDEX IF NOT EXISTS "documents_query_dbl_values"
                    ON "keyspace"."documents" (entries(query_dbl_values))
                    USING 'StorageAttachedIndex';
                    """,
            "documents_query_text_values", """
                    CREATE CUSTOM INDEX IF NOT EXISTS "documents_query_text_values"
                    ON "keyspace"."documents" (entries(query_text_values))
                    USING 'StorageAttachedIndex';
                    """,
            "documents_query_timestamp_values", """
                    CREATE CUSTOM INDEX IF NOT EXISTS "documents_query_timestamp_values"
                    ON "keyspace"."documents" (entries(query_timestamp_values))
                    USING 'StorageAttachedIndex';
                    """,
            "documents_query_null_values", """
                    CREATE CUSTOM INDEX IF NOT EXISTS "documents_query_null_values"
                    ON "keyspace"."documents" (values(query_null_values))
                    USING 'StorageAttachedIndex';
                    """
    );

    private static final Map<String, String> OPTIONAL_INDEXES = Map.of(
            "documents_query_vector_value", """
                    CREATE CUSTOM INDEX IF NOT EXISTS "documents_query_vector_value"
                    ON "keyspace"."documents" (query_vector_value)
                    USING 'StorageAttachedIndex'
                    WITH OPTIONS = {'similarity_function': 'cosine', 'source_model': 'OTHER'};
                    """,
            "documents_query_lexical_value", """
                    CREATE CUSTOM INDEX IF NOT EXISTS "documents_query_lexical_value"
                    ON "keyspace"."documents" (query_lexical_value)
                    USING 'StorageAttachedIndex'
                    WITH OPTIONS = {'index_analyzer': 'standard'};
                    """
    );

    private static final Map<String, String> ALL_INDEXES ;
    static {
        var local = new HashMap<>(REQUIRED_INDEXES);
        local.putAll(OPTIONAL_INDEXES);
        ALL_INDEXES = Collections.unmodifiableMap(local);
    }


    private static String getTableCql(List<SuperShreddingBuilder.SuperShreddingComponent<String>> components){
        return components.stream()
                .filter(component -> component.type() == SuperShreddingBuilder.SuperShreddingComponentType.TABLE)
                .findFirst()
                .map(SuperShreddingBuilder.SuperShreddingComponent::value)
                .orElseThrow(() -> new IllegalArgumentException("No table component found in components list"));
    }

    private static Stream<SuperShreddingBuilder.SuperShreddingComponent<String>> indexComponents(List<SuperShreddingBuilder.SuperShreddingComponent<String>> components){
        return components.stream()
                .filter(component -> component.type() == SuperShreddingBuilder.SuperShreddingComponentType.INDEX);
    }

    private static void assertTableCql(String testName, String expectedCQL, String actualCQL){
        if (LOGGER.isInfoEnabled()){
            LOGGER.info("assertTableCql() - testName: {}, expectedCQL: {}", testName, expectedCQL);
            LOGGER.info("assertTableCql() - testName: {}, actualCQL: {}", testName, actualCQL);
        }
        assertThat(actualCQL)
                .as("Table CQL should be as expected")
                .isEqualTo(SuperShreddingCQL.collapseWhitespace(expectedCQL));
    }

    private static void assertIndexCql(String testName, Map<String, String> expectedCQL, List<SuperShreddingBuilder.SuperShreddingComponent<String>> actualCQL){

        for (var expectedEntry : expectedCQL.entrySet()) {
            var indexName = expectedEntry.getKey();

            LOGGER.info("assertIndexCql() - testName: {}, indexName:{}, expectedCQL: {}", testName, indexName, expectedEntry.getValue());

            var actualComponent = indexComponents(actualCQL)
                    .filter(component -> component.identifier().asInternal().equals(indexName))
                    .findFirst()
                    .orElse(null);

            assertThat(actualComponent)
                    .as("Index component for '%s' should not be null", indexName)
                    .isNotNull();
            LOGGER.info("assertIndexCql() - testName: {}, indexName:{}, actualCQL: {}", testName, indexName, actualComponent.value());

            assertThat(actualComponent.type())
                    .as("Index component for '%s' should be of type INDEX", indexName)
                    .isEqualTo(SuperShreddingBuilder.SuperShreddingComponentType.INDEX);

            assertThat(SuperShreddingCQL.collapseWhitespace(actualComponent.value()))
                    .as("Index CQL for '%s' should be as expected", indexName)
                    .isEqualTo(SuperShreddingCQL.collapseWhitespace(expectedEntry.getValue()));
        }

        var unexpectedIndexes = indexComponents(actualCQL)
                .filter(component -> !expectedCQL.containsKey(component.identifier().asInternal()))
                .toList();
        assertThat(unexpectedIndexes)
                .as("Unexpected indexes found")
                .isEmpty();
    }

    @Test
    public void createTableAllOptional() {

        var builder = SuperShreddingCQLBuilder.cql()
                .withKeyspace(KEYSPACE)
                .withCollection(TABLE)
                .withComment(COMMENT)
                .withVector(1024, "cosine", "OTHER")
                .withLexical("standard");

        var allComponents = builder.build();
        var tableCQL = getTableCql(allComponents);
        assertTableCql("createTableAllOptional", CREATE_TABLE_ALL_OPTIONAL, tableCQL);
        assertIndexCql("createTableAllOptional", ALL_INDEXES, allComponents);
    }

    @Test
    public void createTableNoOptional(){
        var builder = SuperShreddingCQLBuilder.cql()
                .withKeyspace(KEYSPACE)
                .withCollection(TABLE)
                .withComment(COMMENT);

        var allComponents = builder.build();
        var tableCQL = getTableCql(allComponents);
        assertTableCql("createTableAllOptional", CREATE_TABLE_NO_OPTIONAL, tableCQL);
        assertIndexCql("createTableAllOptional", REQUIRED_INDEXES, allComponents);
    }

    @Test
    public void createTableVectorOnly() {
        var builder = SuperShreddingCQLBuilder.cql()
                .withKeyspace(KEYSPACE)
                .withCollection(TABLE)
                .withComment(COMMENT)
                .withVector(1024, "cosine", "OTHER");

        var expectedIndexes = new HashMap<>(REQUIRED_INDEXES);
        expectedIndexes.put("documents_query_vector_value", OPTIONAL_INDEXES.get("documents_query_vector_value"));

        var allComponents = builder.build();
        var tableCQL = getTableCql(allComponents);
        assertTableCql("createTableAllOptional", CREATE_TABLE_VECTOR_ONLY, tableCQL);
        assertIndexCql("createTableAllOptional", expectedIndexes, allComponents);

    }

    @Test
    public void createTableLexicalOnly() {
        var builder = SuperShreddingCQLBuilder.cql()
                .withKeyspace(KEYSPACE)
                .withCollection(TABLE)
                .withComment(COMMENT)
                .withLexical("standard");

        var expectedIndexes = new HashMap<>(REQUIRED_INDEXES);
        expectedIndexes.put("documents_query_lexical_value", OPTIONAL_INDEXES.get("documents_query_lexical_value"));

        var allComponents = builder.build();
        var tableCQL = getTableCql(allComponents);
        assertTableCql("createTableAllOptional", CREATE_TABLE_LEXICAL_ONLY, tableCQL);
        assertIndexCql("createTableAllOptional", expectedIndexes, allComponents);
    }
}
