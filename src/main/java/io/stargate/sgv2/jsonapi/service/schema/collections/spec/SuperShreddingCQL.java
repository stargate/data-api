package io.stargate.sgv2.jsonapi.service.schema.collections.spec;

import org.apache.commons.text.StringSubstitutor;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface SuperShreddingCQL {

    static String collapseWhitespace(String s) {
      return s.replaceAll("\\s+", " ").trim();
    }

    interface CQL {
        String CREATE_TABLE_TEMPLATE =
                """
                        CREATE TABLE ${IF_NOT_EXISTS:-} ${KEYSPACE}.${TABLE} (
                            "key"                     tuple<tinyint, text> PRIMARY KEY,
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
                        ) ${COMMENT_CLAUSE:-};
                        """;

        String TABLE_VECTOR_COLUMN_TEMPLATE =
                """
                        "query_vector_value"      vector<float, ${VECTOR_DIM}>,""";

        String TABLE_LEXICAL_COLUMN_TEMPLATE =
                """
                        "query_lexical_value"     text,""";

        String TABLE_COMMENT_CLAUSE_TEMPLATE =
                """
                        WITH comment = '${COMMENT}'""";

        String INDEX_EXIST_KEYS_TEMPLATE =
                """
                        CREATE CUSTOM INDEX ${IF_NOT_EXISTS:-} ${TABLE}_exist_keys
                        ON ${KEYSPACE}.${TABLE} (values("exist_keys"))
                        USING 'StorageAttachedIndex';
                        """;

        String INDEX_ARRAY_SIZE_TEMPLATE =
                """
                        CREATE CUSTOM INDEX ${IF_NOT_EXISTS:-} ${TABLE}_array_size
                        ON ${KEYSPACE}.${TABLE} (entries("array_size"))
                        USING 'StorageAttachedIndex';
                        """;

        String INDEX_ARRAY_CONTAINS_TEMPLATE =
                """
                        CREATE CUSTOM INDEX ${IF_NOT_EXISTS:-} ${TABLE}_array_contains
                        ON ${KEYSPACE}.${TABLE} (values("array_contains"))
                        USING 'StorageAttachedIndex';
                        """;

        String INDEX_QUERY_BOOLEAN_VALUES_TEMPLATE =
                """
                        CREATE CUSTOM INDEX ${IF_NOT_EXISTS:-} ${TABLE}_query_bool_values
                        ON ${KEYSPACE}.${TABLE} (entries("query_bool_values"))
                        USING 'StorageAttachedIndex';
                        """;

        String INDEX_QUERY_DBL_VALUES_TEMPLATE =
                """
                        CREATE CUSTOM INDEX ${IF_NOT_EXISTS:-} ${TABLE}_query_dbl_values
                        ON ${KEYSPACE}.${TABLE} (entries("query_dbl_values"))
                        USING 'StorageAttachedIndex';
                        """;

        String INDEX_QUERY_TEXT_VALUES_TEMPLATE =
                """
                        CREATE CUSTOM INDEX ${IF_NOT_EXISTS:-} ${TABLE}_query_text_values
                        ON ${KEYSPACE}.${TABLE} (entries("query_text_values"))
                        USING 'StorageAttachedIndex';
                        """;

        String INDEX_QUERY_TIMESTAMP_VALUES_TEMPLATE =
                """
                        CREATE CUSTOM INDEX ${IF_NOT_EXISTS:-} ${TABLE}_query_timestamp_values
                        ON ${KEYSPACE}.${TABLE} (entries("query_timestamp_values"))
                        USING 'StorageAttachedIndex';
                        """;

        String INDEX_QUERY_NULL_VALUES_TEMPLATE =
                """
                        CREATE CUSTOM INDEX ${IF_NOT_EXISTS:-} ${TABLE}_query_null_values
                        ON ${KEYSPACE}.${TABLE} (values("query_null_values"))
                        USING 'StorageAttachedIndex';
                        """;

        String INDEX_QUERY_VECTOR_VALUE_TEMPLATE =
                """
                        CREATE CUSTOM INDEX ${IF_NOT_EXISTS:-} ${TABLE}_query_vector_value
                        ON ${KEYSPACE}.${TABLE} ("query_vector_value")
                        USING 'StorageAttachedIndex'
                        ${VECTOR_WITH_OPTIONS:-};
                        """;

        String VECTOR_WITH_OPTIONS_TEMPLATE =
                """
                        WITH OPTIONS = {'similarity_function': '${similarity_function}', 'source_model': '${source_model}'}
                        """.trim();;

        String INDEX_QUERY_LEXICAL_VALUE_TEMPLATE =
                """
                        CREATE CUSTOM INDEX ${IF_NOT_EXISTS:-} ${TABLE}_query_lexical_value
                        ON ${KEYSPACE}.${TABLE} ("query_lexical_value")
                        USING 'StorageAttachedIndex'
                        ${LEXICAL_WITH_OPTIONS:-};
                        """;

        String LEXICAL_WITH_OPTIONS_TEMPLATE =
                """
                        WITH OPTIONS = {'index_analyzer': '${index_analyzer}'}
                        """.trim();

        List<String> ALL_INDEXES = List.of(
                INDEX_EXIST_KEYS_TEMPLATE,         INDEX_ARRAY_SIZE_TEMPLATE,          INDEX_ARRAY_CONTAINS_TEMPLATE,
                INDEX_QUERY_BOOLEAN_VALUES_TEMPLATE, INDEX_QUERY_DBL_VALUES_TEMPLATE,  INDEX_QUERY_TEXT_VALUES_TEMPLATE,
                INDEX_QUERY_TIMESTAMP_VALUES_TEMPLATE, INDEX_QUERY_NULL_VALUES_TEMPLATE,
                INDEX_QUERY_VECTOR_VALUE_TEMPLATE,  INDEX_QUERY_LEXICAL_VALUE_TEMPLATE);

        List<String> OPTIONAL_INDEXES = List.of(INDEX_QUERY_VECTOR_VALUE_TEMPLATE, INDEX_QUERY_LEXICAL_VALUE_TEMPLATE);
        List<String> REQUIRED_INDEXES = SuperShreddingMetadata.listDifference(ALL_INDEXES, OPTIONAL_INDEXES);
    }

    record ClauseTemplate(String template, String toKeyName) {

        public Optional<String> format(Map<String, String> values) {
            if (values == null || values.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new StringSubstitutor(values).replace(template));
        }
    }

    record IndexCQLAndDef(String cql, SuperShreddingMetadata.IndexDef indexDef, ClauseTemplate clauseTemplate) {
        public IndexCQLAndDef(String cql, SuperShreddingMetadata.IndexDef indexDef) {
            this(cql, indexDef, null);
        }
    }

    interface IndexCQLAndDefs {
        IndexCQLAndDef INDEX_EXIST_KEYS             = new IndexCQLAndDef(CQL.INDEX_EXIST_KEYS_TEMPLATE,              SuperShreddingMetadata.IndexDefs.EXIST_KEYS);
        IndexCQLAndDef INDEX_ARRAY_SIZE             = new IndexCQLAndDef(CQL.INDEX_ARRAY_SIZE_TEMPLATE,              SuperShreddingMetadata.IndexDefs.ARRAY_SIZE);
        IndexCQLAndDef INDEX_ARRAY_CONTAINS         = new IndexCQLAndDef(CQL.INDEX_ARRAY_CONTAINS_TEMPLATE,          SuperShreddingMetadata.IndexDefs.ARRAY_CONTAINS);
        IndexCQLAndDef INDEX_QUERY_BOOL_VALUES      = new IndexCQLAndDef(CQL.INDEX_QUERY_BOOLEAN_VALUES_TEMPLATE,    SuperShreddingMetadata.IndexDefs.QUERY_BOOLEAN_VALUES);
        IndexCQLAndDef INDEX_QUERY_DBL_VALUES       = new IndexCQLAndDef(CQL.INDEX_QUERY_DBL_VALUES_TEMPLATE,        SuperShreddingMetadata.IndexDefs.QUERY_DOUBLE_VALUES);
        IndexCQLAndDef INDEX_QUERY_TEXT_VALUES      = new IndexCQLAndDef(CQL.INDEX_QUERY_TEXT_VALUES_TEMPLATE,       SuperShreddingMetadata.IndexDefs.QUERY_TEXT_VALUES);
        IndexCQLAndDef INDEX_QUERY_TIMESTAMP_VALUES = new IndexCQLAndDef(CQL.INDEX_QUERY_TIMESTAMP_VALUES_TEMPLATE,  SuperShreddingMetadata.IndexDefs.QUERY_TIMESTAMP_VALUES);
        IndexCQLAndDef INDEX_QUERY_NULL_VALUES      = new IndexCQLAndDef(CQL.INDEX_QUERY_NULL_VALUES_TEMPLATE,       SuperShreddingMetadata.IndexDefs.QUERY_NULL_VALUES);

        IndexCQLAndDef INDEX_QUERY_VECTOR_VALUE  = new IndexCQLAndDef(
                CQL.INDEX_QUERY_VECTOR_VALUE_TEMPLATE,
                SuperShreddingMetadata.IndexDefs.QUERY_VECTOR_VALUE,
                new ClauseTemplate(CQL.VECTOR_WITH_OPTIONS_TEMPLATE, "VECTOR_WITH_OPTIONS"));

        IndexCQLAndDef INDEX_QUERY_LEXICAL_VALUE = new IndexCQLAndDef(
                CQL.INDEX_QUERY_LEXICAL_VALUE_TEMPLATE,
                SuperShreddingMetadata.IndexDefs.QUERY_LEXICAL_VALUE,
                new ClauseTemplate(CQL.LEXICAL_WITH_OPTIONS_TEMPLATE, "LEXICAL_WITH_OPTIONS"));

        List<IndexCQLAndDef> ALL_INDEXES = List.of(
                INDEX_EXIST_KEYS,             INDEX_ARRAY_SIZE,            INDEX_ARRAY_CONTAINS,
                INDEX_QUERY_BOOL_VALUES,      INDEX_QUERY_DBL_VALUES,      INDEX_QUERY_TEXT_VALUES,
                INDEX_QUERY_TIMESTAMP_VALUES, INDEX_QUERY_NULL_VALUES,
                INDEX_QUERY_VECTOR_VALUE,     INDEX_QUERY_LEXICAL_VALUE);
        List<IndexCQLAndDef> OPTIONAL_INDEXES = List.of(INDEX_QUERY_VECTOR_VALUE, INDEX_QUERY_LEXICAL_VALUE);
        List<IndexCQLAndDef> REQUIRED_INDEXES = SuperShreddingMetadata.listDifference(ALL_INDEXES, OPTIONAL_INDEXES);

        Map<SuperShreddingMetadata.IndexDef, IndexCQLAndDef> ALL_INDEXES_BY_INDEX_DEF = ALL_INDEXES.stream()
                .collect(Collectors.toMap(IndexCQLAndDef::indexDef, Function.identity()));

    }
}
