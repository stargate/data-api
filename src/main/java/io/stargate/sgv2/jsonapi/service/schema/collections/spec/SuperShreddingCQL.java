package io.stargate.sgv2.jsonapi.service.schema.collections.spec;

import org.apache.commons.text.StringSubstitutor;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingMetadata.IndexDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingMetadata.IndexDefs;

import static io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingMetadata.listDifference;

/**
 * Defines the dynamic CQL built by the {@link SuperShreddingCQLBuilder}.
 * DO NOT MAKE changes to the CQL without testing, in many cases it has spaces and
 * capitalization specifically designed to match what is created by parts of the driver.
 * <p>
 *  <b>NOTE:</b> we do not use this in production, where we use the driver
 *  schema builder, this is for testing. See {@link SuperShreddingBuilder} for the testing
 *  process.
 * </p>
 * <p>
 *  The tempalates use the {@link StringSubstitutor} and in particular use the idea of a default if
 *  the key is not present. <code>${VECTOR_COLUMN:-}</code> is an example, if not present an empty string
 *  is put in place of the include.
 * </p>
 */
public interface SuperShreddingCQL {

    /**
     * Collapses all reg ex white space characters to a single space, so we can compare strings.
     */
    static String collapseWhitespace(String s) {
      return s.replaceAll("\\s+", " ").trim();
    }

    /**
     * CQL templates for a dynamic super shredding table.
     */
    interface CQL {
        // NOTE: frozen<> included on tuple type because the auto gen for TableMetadata will
        // result in TupleType adding frozen, because all tuples are implicitly frozen
        // this has no real effect.
        // NOTE: pls keep the order following the SuperShreddingMetadata
        String CREATE_TABLE_TEMPLATE =
                """
                        CREATE TABLE ${IF_NOT_EXISTS:-} ${KEYSPACE}.${TABLE} (
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
                            ${VECTOR_COLUMN:-}
                            ${LEXICAL_COLUMN:-}
                            PRIMARY KEY ("key")
                        )${COMMENT_CLAUSE:-};
                        """;

        String TABLE_VECTOR_COLUMN_TEMPLATE =
                """
                        "query_vector_value"      vector<float, ${VECTOR_DIM}>,""";

        String TABLE_LEXICAL_COLUMN_TEMPLATE =
                """
                        "query_lexical_value"     text,""";

        String TABLE_COMMENT_CLAUSE_TEMPLATE =
                """
                        WITH comment = '${COMMENT}'\
                       """;

        String INDEX_EXIST_KEYS_TEMPLATE =
                """
                        CREATE CUSTOM INDEX ${IF_NOT_EXISTS:-} "${TABLE}_exist_keys"
                        ON "${KEYSPACE}"."${TABLE}" (values("exist_keys"))
                        USING 'StorageAttachedIndex';
                        """;

        String INDEX_ARRAY_SIZE_TEMPLATE =
                """
                        CREATE CUSTOM INDEX ${IF_NOT_EXISTS:-} "${TABLE}_array_size"
                        ON "${KEYSPACE}"."${TABLE}" (entries("array_size"))
                        USING 'StorageAttachedIndex';
                        """;

        String INDEX_ARRAY_CONTAINS_TEMPLATE =
                """
                        CREATE CUSTOM INDEX ${IF_NOT_EXISTS:-} "${TABLE}_array_contains"
                        ON "${KEYSPACE}"."${TABLE}" (values("array_contains"))
                        USING 'StorageAttachedIndex';
                        """;

        String INDEX_QUERY_BOOLEAN_VALUES_TEMPLATE =
                """
                        CREATE CUSTOM INDEX ${IF_NOT_EXISTS:-} "${TABLE}_query_bool_values"
                        ON "${KEYSPACE}"."${TABLE}" (entries("query_bool_values"))
                        USING 'StorageAttachedIndex';
                        """;

        String INDEX_QUERY_DBL_VALUES_TEMPLATE =
                """
                        CREATE CUSTOM INDEX ${IF_NOT_EXISTS:-} "${TABLE}_query_dbl_values"
                        ON "${KEYSPACE}"."${TABLE}" (entries("query_dbl_values"))
                        USING 'StorageAttachedIndex';
                        """;

        String INDEX_QUERY_TEXT_VALUES_TEMPLATE =
                """
                        CREATE CUSTOM INDEX ${IF_NOT_EXISTS:-} "${TABLE}_query_text_values"
                        ON "${KEYSPACE}"."${TABLE}" (entries("query_text_values"))
                        USING 'StorageAttachedIndex';
                        """;

        String INDEX_QUERY_TIMESTAMP_VALUES_TEMPLATE =
                """
                        CREATE CUSTOM INDEX ${IF_NOT_EXISTS:-} "${TABLE}_query_timestamp_values"
                        ON "${KEYSPACE}"."${TABLE}" (entries("query_timestamp_values"))
                        USING 'StorageAttachedIndex';
                        """;

        String INDEX_QUERY_NULL_VALUES_TEMPLATE =
                """
                        CREATE CUSTOM INDEX ${IF_NOT_EXISTS:-} "${TABLE}_query_null_values"
                        ON "${KEYSPACE}"."${TABLE}" (values("query_null_values"))
                        USING 'StorageAttachedIndex';
                        """;

        String INDEX_QUERY_VECTOR_VALUE_TEMPLATE =
                """
                        CREATE CUSTOM INDEX ${IF_NOT_EXISTS:-} "${TABLE}_query_vector_value"
                        ON "${KEYSPACE}"."${TABLE}" ("query_vector_value")
                        USING 'StorageAttachedIndex'
                        ${VECTOR_WITH_OPTIONS:-};
                        """;

        String VECTOR_WITH_OPTIONS_TEMPLATE =
                """
                        WITH OPTIONS = { 'similarity_function' : '${similarity_function}', 'source_model' : '${source_model}'}
                        """.trim();

        String INDEX_QUERY_LEXICAL_VALUE_TEMPLATE =
                """
                        CREATE CUSTOM INDEX ${IF_NOT_EXISTS:-} "${TABLE}_query_lexical_value"
                        ON "${KEYSPACE}"."${TABLE}" ("query_lexical_value")
                        USING 'StorageAttachedIndex'
                        ${LEXICAL_WITH_OPTIONS:-};
                        """;

        String LEXICAL_WITH_OPTIONS_TEMPLATE =
                """
                        WITH OPTIONS = { 'index_analyzer' : '${index_analyzer}'}
                        """.trim();

        List<String> ALL_INDEXES = List.of(
                INDEX_EXIST_KEYS_TEMPLATE,         INDEX_ARRAY_SIZE_TEMPLATE,          INDEX_ARRAY_CONTAINS_TEMPLATE,
                INDEX_QUERY_BOOLEAN_VALUES_TEMPLATE, INDEX_QUERY_DBL_VALUES_TEMPLATE,  INDEX_QUERY_TEXT_VALUES_TEMPLATE,
                INDEX_QUERY_TIMESTAMP_VALUES_TEMPLATE, INDEX_QUERY_NULL_VALUES_TEMPLATE,
                INDEX_QUERY_VECTOR_VALUE_TEMPLATE,  INDEX_QUERY_LEXICAL_VALUE_TEMPLATE);

        List<String> OPTIONAL_INDEXES = List.of(INDEX_QUERY_VECTOR_VALUE_TEMPLATE, INDEX_QUERY_LEXICAL_VALUE_TEMPLATE);
        List<String> REQUIRED_INDEXES = listDifference(ALL_INDEXES, OPTIONAL_INDEXES);
    }

    /**
     * Holder for a template that generates a clause, such as `VECTOR_WITH_OPTIONS_TEMPLATE` above.
     * @param template The template we need to run to get the value for the clause.
     * @param toKeyName the key the result of the template should be assigned to when used to
     *                  format the CREATE TABLE statement.
     */
    record ClauseTemplate(String template, String toKeyName) {

        public Optional<String> format(Map<String, String> values) {
            if (values == null || values.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new StringSubstitutor(values).replace(template));
        }
    }

    /**
     * Holder to associate the definition of the index from {@link IndexDefs} with the
     * CQL here to create it, and optionally the template to make a sub clause for the index.
     */
    record IndexCQLAndDef(String cql, IndexDef indexDef, ClauseTemplate clauseTemplate) {

        public IndexCQLAndDef(String cql, IndexDef indexDef) {
            this(cql, indexDef, null);
        }
    }

    /**
     * Associates the CQL defined above with the index from {@link IndexDefs}
     * it is designed to create.
     */
    interface IndexCQLAndDefs {

        // Required Indexes
        IndexCQLAndDef INDEX_EXIST_KEYS             = new IndexCQLAndDef(CQL.INDEX_EXIST_KEYS_TEMPLATE,              IndexDefs.EXIST_KEYS);
        IndexCQLAndDef INDEX_ARRAY_SIZE             = new IndexCQLAndDef(CQL.INDEX_ARRAY_SIZE_TEMPLATE,              IndexDefs.ARRAY_SIZE);
        IndexCQLAndDef INDEX_ARRAY_CONTAINS         = new IndexCQLAndDef(CQL.INDEX_ARRAY_CONTAINS_TEMPLATE,          IndexDefs.ARRAY_CONTAINS);
        IndexCQLAndDef INDEX_QUERY_BOOL_VALUES      = new IndexCQLAndDef(CQL.INDEX_QUERY_BOOLEAN_VALUES_TEMPLATE,    IndexDefs.QUERY_BOOLEAN_VALUES);
        IndexCQLAndDef INDEX_QUERY_DBL_VALUES       = new IndexCQLAndDef(CQL.INDEX_QUERY_DBL_VALUES_TEMPLATE,        IndexDefs.QUERY_DOUBLE_VALUES);
        IndexCQLAndDef INDEX_QUERY_TEXT_VALUES      = new IndexCQLAndDef(CQL.INDEX_QUERY_TEXT_VALUES_TEMPLATE,       IndexDefs.QUERY_TEXT_VALUES);
        IndexCQLAndDef INDEX_QUERY_TIMESTAMP_VALUES = new IndexCQLAndDef(CQL.INDEX_QUERY_TIMESTAMP_VALUES_TEMPLATE,  IndexDefs.QUERY_TIMESTAMP_VALUES);
        IndexCQLAndDef INDEX_QUERY_NULL_VALUES      = new IndexCQLAndDef(CQL.INDEX_QUERY_NULL_VALUES_TEMPLATE,       IndexDefs.QUERY_NULL_VALUES);

        // Optional Indexes
        IndexCQLAndDef INDEX_QUERY_VECTOR_VALUE  = new IndexCQLAndDef(
                CQL.INDEX_QUERY_VECTOR_VALUE_TEMPLATE,
                IndexDefs.QUERY_VECTOR_VALUE,
                new ClauseTemplate(CQL.VECTOR_WITH_OPTIONS_TEMPLATE, "VECTOR_WITH_OPTIONS"));

        IndexCQLAndDef INDEX_QUERY_LEXICAL_VALUE = new IndexCQLAndDef(
                CQL.INDEX_QUERY_LEXICAL_VALUE_TEMPLATE,
                IndexDefs.QUERY_LEXICAL_VALUE,
                new ClauseTemplate(CQL.LEXICAL_WITH_OPTIONS_TEMPLATE, "LEXICAL_WITH_OPTIONS"));

        List<IndexCQLAndDef> ALL_INDEXES = List.of(
                INDEX_EXIST_KEYS,             INDEX_ARRAY_SIZE,            INDEX_ARRAY_CONTAINS,
                INDEX_QUERY_BOOL_VALUES,      INDEX_QUERY_DBL_VALUES,      INDEX_QUERY_TEXT_VALUES,
                INDEX_QUERY_TIMESTAMP_VALUES, INDEX_QUERY_NULL_VALUES,
                INDEX_QUERY_VECTOR_VALUE,     INDEX_QUERY_LEXICAL_VALUE);
        List<IndexCQLAndDef> OPTIONAL_INDEXES = List.of(INDEX_QUERY_VECTOR_VALUE, INDEX_QUERY_LEXICAL_VALUE);
        List<IndexCQLAndDef> REQUIRED_INDEXES = listDifference(ALL_INDEXES, OPTIONAL_INDEXES);

        Map<IndexDef, IndexCQLAndDef> ALL_INDEXES_BY_INDEX_DEF = ALL_INDEXES.stream()
                .collect(Collectors.toMap(IndexCQLAndDef::indexDef, Function.identity()));

    }
}
