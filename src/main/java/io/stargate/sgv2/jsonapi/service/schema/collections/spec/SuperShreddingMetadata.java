// @formatter:off
package io.stargate.sgv2.jsonapi.service.schema.collections.spec;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexKind;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultColumnMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultIndexMetadata;
import com.datastax.oss.driver.internal.querybuilder.schema.DefaultCreateIndex;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import io.stargate.sgv2.jsonapi.config.constants.VectorConstants;
import io.stargate.sgv2.jsonapi.service.cqldriver.override.ExtendedCreateIndex;
import io.stargate.sgv2.jsonapi.service.cqldriver.override.ExtendedVectorType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexFunction;
import io.stargate.sgv2.jsonapi.service.schema.tables.CQLSAIIndex;
import io.stargate.sgv2.jsonapi.util.ColumnMetadataPredicate;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Names of columns in Document-containing Tables
 *
 * <p>Prev comments:
 *
 * <pre>
 *
 *           Atomic values are added to the array_contains field to support $eq on both atomic value and
 *           array element
 *
 * String ARRAY_CONTAINS = "array_contains";
 *
 * Text map support _id $ne and _id $nin on both atomic value and array element
 *         String QUERY_TEXT_VALUES = "query_text_values";
 *
 *         Physical table column name that stores the vector field.
 *  String QUERY_VECTOR_VALUE = "query_vector_value";
 *
 *  Physical table column name that stores the lexical content.
 *  String QUERY_LEXICAL_VALUE = "query_lexical_value";
 *  </pre>
 */
public interface SuperShreddingMetadata {

  static <T> List<T> listDifference(List<T> list1, List<T> list2) {
    return list1.stream().filter(item -> !list2.contains(item)).collect(Collectors.toList());
  }

  /**
   * String names of all columns, in the order that we traditionally have them in the collection
   * table, pls try to keep the order :)
   */
  interface Names {

    // Required columns
    String KEY = "key";
    String TX_ID = "tx_id";
    String DOC_JSON = "doc_json";
    String EXIST_KEYS = "exist_keys";
    String ARRAY_SIZE = "array_size";
    String ARRAY_CONTAINS = "array_contains";
    String QUERY_BOOLEAN_VALUES = "query_bool_values";
    String QUERY_DOUBLE_VALUES = "query_dbl_values";
    String QUERY_TEXT_VALUES = "query_text_values";
    String QUERY_TIMESTAMP_VALUES = "query_timestamp_values";
    String QUERY_NULL_VALUES = "query_null_values";
    // Optional columns
    String QUERY_VECTOR_VALUE = "query_vector_value";
    String QUERY_LEXICAL_VALUE = "query_lexical_value";

    List<String> ALL =
        List.of(
            KEY,
            TX_ID,
            DOC_JSON,
            EXIST_KEYS,
            ARRAY_SIZE,
            ARRAY_CONTAINS,
            QUERY_BOOLEAN_VALUES,
            QUERY_DOUBLE_VALUES,
            QUERY_NULL_VALUES,
            QUERY_TEXT_VALUES,
            QUERY_TIMESTAMP_VALUES,
            QUERY_VECTOR_VALUE,
            QUERY_LEXICAL_VALUE);
    List<String> PARTITION_KEY = List.of(KEY);
    List<String> ALL_REGULAR_COLUMNS = listDifference(ALL, PARTITION_KEY);
    List<String> OPTIONAL = List.of(QUERY_VECTOR_VALUE, QUERY_LEXICAL_VALUE);
    List<String> REQUIRED = listDifference(ALL_REGULAR_COLUMNS, OPTIONAL);
  }

  interface Identifiers {

    // Required columns
    CqlIdentifier KEY = CqlIdentifier.fromInternal(Names.KEY);
    CqlIdentifier TX_ID = CqlIdentifier.fromInternal(Names.TX_ID);
    CqlIdentifier DOC_JSON = CqlIdentifier.fromInternal(Names.DOC_JSON);
    CqlIdentifier EXIST_KEYS = CqlIdentifier.fromInternal(Names.EXIST_KEYS);
    CqlIdentifier ARRAY_SIZE = CqlIdentifier.fromInternal(Names.ARRAY_SIZE);
    CqlIdentifier ARRAY_CONTAINS = CqlIdentifier.fromInternal(Names.ARRAY_CONTAINS);
    CqlIdentifier QUERY_BOOLEAN_VALUES = CqlIdentifier.fromInternal(Names.QUERY_BOOLEAN_VALUES);
    CqlIdentifier QUERY_DOUBLE_VALUES = CqlIdentifier.fromInternal(Names.QUERY_DOUBLE_VALUES);
    CqlIdentifier QUERY_TEXT_VALUES = CqlIdentifier.fromInternal(Names.QUERY_TEXT_VALUES);
    CqlIdentifier QUERY_TIMESTAMP_VALUES = CqlIdentifier.fromInternal(Names.QUERY_TIMESTAMP_VALUES);
    CqlIdentifier QUERY_NULL_VALUES = CqlIdentifier.fromInternal(Names.QUERY_NULL_VALUES);
    // Optional columns
    CqlIdentifier QUERY_VECTOR_VALUE = CqlIdentifier.fromInternal(Names.QUERY_VECTOR_VALUE);
    CqlIdentifier QUERY_LEXICAL_VALUE = CqlIdentifier.fromInternal(Names.QUERY_LEXICAL_VALUE);

    List<CqlIdentifier> ALL =
        List.of(
            KEY,
            TX_ID,
            DOC_JSON,
            EXIST_KEYS,
            ARRAY_SIZE,
            ARRAY_CONTAINS,
            QUERY_BOOLEAN_VALUES,
            QUERY_DOUBLE_VALUES,
            QUERY_NULL_VALUES,
            QUERY_TEXT_VALUES,
            QUERY_TIMESTAMP_VALUES,
            QUERY_VECTOR_VALUE,
            QUERY_LEXICAL_VALUE);
    List<CqlIdentifier> PARTITION_KEY = List.of(KEY);
    List<CqlIdentifier> ALL_REGULAR_COLUMNS = listDifference(ALL, PARTITION_KEY);
    List<CqlIdentifier> OPTIONAL = List.of(QUERY_VECTOR_VALUE, QUERY_LEXICAL_VALUE);
    List<CqlIdentifier> REQUIRED = listDifference(ALL_REGULAR_COLUMNS, OPTIONAL);
  }

  @FunctionalInterface
  interface ColumnMetadataFactory {
    ColumnMetadata columnMetadata(
        ColumnDef columnDef,
        CqlIdentifier keyspace,
        CqlIdentifier collection,
        Map<String, Object> options);
  }

  record ColumnDef(CqlIdentifier name, DataType type, ColumnMetadataFactory metadataFactory) {

    ColumnDef(CqlIdentifier name, DataType type) {
      this(name, type, null);
    }

    public ColumnMetadata columnMetadata(
        CqlIdentifier keyspace, CqlIdentifier collection, Map<String, Object> perColumnOptions) {
      if (metadataFactory == null) {
        if (perColumnOptions != null && !perColumnOptions.isEmpty()) {
          throw new IllegalArgumentException(
              "Cannot specify perColumnOptions if the columnDef does not have a metadataFactory");
        }

        return new DefaultColumnMetadata(keyspace, collection, name, type, false);
      }
      var factoryValue =
          metadataFactory.columnMetadata(this, keyspace, collection, perColumnOptions);
      Objects.requireNonNull(
          factoryValue, "ColumnMetadataFactory returned null for columnDef.name:{}" + name);
      return factoryValue;
    }

    public CreateTable addTo(CreateTable createTable) {
      return createTable.withColumn(name, type);
    }

    public ColumnMetadataPredicate predicate() {
      return new ColumnMetadataPredicate.Basic(name, type);
    }
  }

  interface ColumnDefs {

    // Required columns
    ColumnDef KEY =
        new ColumnDef(Identifiers.KEY, DataTypes.tupleOf(DataTypes.TINYINT, DataTypes.TEXT));
    ColumnDef TX_ID = new ColumnDef(Identifiers.TX_ID, DataTypes.TIMEUUID);
    ColumnDef DOC_JSON = new ColumnDef(Identifiers.DOC_JSON, DataTypes.TEXT);
    ColumnDef EXIST_KEYS = new ColumnDef(Identifiers.EXIST_KEYS, DataTypes.setOf(DataTypes.TEXT));
    ColumnDef ARRAY_SIZE =
        new ColumnDef(Identifiers.ARRAY_SIZE, DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT));
    ColumnDef ARRAY_CONTAINS =
        new ColumnDef(Identifiers.ARRAY_CONTAINS, DataTypes.setOf(DataTypes.TEXT));
    ColumnDef QUERY_BOOLEAN_VALUES =
        new ColumnDef(
            Identifiers.QUERY_BOOLEAN_VALUES, DataTypes.mapOf(DataTypes.TEXT, DataTypes.TINYINT));
    ColumnDef QUERY_DOUBLE_VALUES =
        new ColumnDef(
            Identifiers.QUERY_DOUBLE_VALUES, DataTypes.mapOf(DataTypes.TEXT, DataTypes.DECIMAL));
    ColumnDef QUERY_TEXT_VALUES =
        new ColumnDef(
            Identifiers.QUERY_TEXT_VALUES, DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT));
    ColumnDef QUERY_TIMESTAMP_VALUES =
        new ColumnDef(
            Identifiers.QUERY_TIMESTAMP_VALUES,
            DataTypes.mapOf(DataTypes.TEXT, DataTypes.TIMESTAMP));
    ColumnDef QUERY_NULL_VALUES =
        new ColumnDef(Identifiers.QUERY_NULL_VALUES, DataTypes.setOf(DataTypes.TEXT));
    // Optional columns
    // NOTE: using our extended vector, length is dependent on the vector dimension of the
    // collection
    ColumnDef QUERY_VECTOR_VALUE =
        new ColumnDef(
            Identifiers.QUERY_VECTOR_VALUE,
            new ExtendedVectorType(DataTypes.FLOAT, 1),
            new ColumnMetadataFactory() {
              @Override
              public ColumnMetadata columnMetadata(
                  ColumnDef columnDef,
                  CqlIdentifier keyspace,
                  CqlIdentifier collection,
                  Map<String, Object> options) {

                Objects.requireNonNull(options, "options cannot be null");
                Integer dimension = (Integer) options.get("dimensions");
                if (dimension == null) {
                  throw new IllegalArgumentException(
                      "`dimensions` is required option for vector column");
                }
                var elementType =
                    ((ExtendedVectorType) ColumnDefs.QUERY_VECTOR_VALUE.type()).getElementType();
                var vectorWithDimension = new ExtendedVectorType(elementType, dimension);

                return new DefaultColumnMetadata(
                    keyspace, collection, columnDef.name(), vectorWithDimension, false);
              }
            });
    ColumnDef QUERY_LEXICAL_VALUE = new ColumnDef(Identifiers.QUERY_LEXICAL_VALUE, DataTypes.TEXT);

    List<ColumnDef> ALL =
        List.of(
            KEY,
            TX_ID,
            DOC_JSON,
            EXIST_KEYS,
            ARRAY_SIZE,
            ARRAY_CONTAINS,
            QUERY_BOOLEAN_VALUES,
            QUERY_DOUBLE_VALUES,
            QUERY_TEXT_VALUES,
            QUERY_TIMESTAMP_VALUES,
            QUERY_NULL_VALUES,
            QUERY_VECTOR_VALUE,
            QUERY_LEXICAL_VALUE);
    List<ColumnDef> PARTITION_KEY = List.of(KEY);
    List<ColumnDef> ALL_REGULAR_COLUMNS = listDifference(ALL, PARTITION_KEY);
    List<ColumnDef> OPTIONAL = List.of(QUERY_VECTOR_VALUE, QUERY_LEXICAL_VALUE);
    List<ColumnDef> REQUIRED = listDifference(ALL_REGULAR_COLUMNS, OPTIONAL);

    static Stream<ColumnMetadata> toColumnMetadata(
        CqlIdentifier keyspace, CqlIdentifier table, List<ColumnDef> columns) {
      return toColumnMetadata(keyspace, table, columns, Collections.emptyMap());
    }

    static Stream<ColumnMetadata> toColumnMetadata(
        CqlIdentifier keyspace,
        CqlIdentifier table,
        List<ColumnDef> columnDefs,
        Map<ColumnDef, Map<String, Object>> perColumnOptions) {

      Map<ColumnDef, Map<String, Object>> safeOptions =
          perColumnOptions != null ? perColumnOptions : Collections.emptyMap();
      return columnDefs.stream()
          .map(columnDef -> columnDef.columnMetadata(keyspace, table, safeOptions.get(columnDef)));
    }
  }

  interface Predicates {

    // Required columns
    ColumnMetadataPredicate KEY = ColumnDefs.KEY.predicate();
    ColumnMetadataPredicate TX_ID = ColumnDefs.TX_ID.predicate();
    ColumnMetadataPredicate DOC_JSON = ColumnDefs.DOC_JSON.predicate();
    ColumnMetadataPredicate EXIST_KEYS = ColumnDefs.EXIST_KEYS.predicate();
    ColumnMetadataPredicate ARRAY_SIZE = ColumnDefs.ARRAY_SIZE.predicate();
    ColumnMetadataPredicate ARRAY_CONTAINS = ColumnDefs.ARRAY_CONTAINS.predicate();
    ColumnMetadataPredicate QUERY_BOOLEAN_VALUES = ColumnDefs.QUERY_BOOLEAN_VALUES.predicate();
    ColumnMetadataPredicate QUERY_DOUBLE_VALUES = ColumnDefs.QUERY_DOUBLE_VALUES.predicate();
    ColumnMetadataPredicate QUERY_TEXT_VALUES = ColumnDefs.QUERY_TEXT_VALUES.predicate();
    ColumnMetadataPredicate QUERY_TIMESTAMP_VALUES = ColumnDefs.QUERY_TIMESTAMP_VALUES.predicate();
    ColumnMetadataPredicate QUERY_NULL_VALUES = ColumnDefs.QUERY_NULL_VALUES.predicate();
    // Optional columns
    // NOTE: using our extended vector, length is dependent on the vector dimension of the
    // collection
    ColumnMetadataPredicate QUERY_VECTOR_VALUE =
        new ColumnMetadataPredicate.Vector(
            ColumnDefs.QUERY_VECTOR_VALUE.name(),
            ((ExtendedVectorType) ColumnDefs.QUERY_VECTOR_VALUE.type()).getElementType());
    ColumnMetadataPredicate QUERY_LEXICAL_VALUE = ColumnDefs.QUERY_LEXICAL_VALUE.predicate();

    List<ColumnMetadataPredicate> ALL =
        List.of(
            KEY,
            TX_ID,
            DOC_JSON,
            EXIST_KEYS,
            ARRAY_SIZE,
            ARRAY_CONTAINS,
            QUERY_BOOLEAN_VALUES,
            QUERY_DOUBLE_VALUES,
            QUERY_TEXT_VALUES,
            QUERY_TIMESTAMP_VALUES,
            QUERY_NULL_VALUES,
            QUERY_VECTOR_VALUE,
            QUERY_LEXICAL_VALUE);
    List<ColumnMetadataPredicate> PARTITION_KEY = List.of(KEY);
    List<ColumnMetadataPredicate> OPTIONAL = List.of(QUERY_VECTOR_VALUE, QUERY_LEXICAL_VALUE);
    List<ColumnMetadataPredicate> REQUIRED = listDifference(ALL, OPTIONAL);

    static List<ColumnMetadataPredicate> allFailingPredicates(
        List<ColumnMetadataPredicate> predicates, Collection<ColumnMetadata> columns) {
      return predicates.stream()
          .filter(predicate -> columns.stream().noneMatch(predicate))
          .toList();
    }

    static List<ColumnMetadata> allUnexpectedColumns(
        List<ColumnMetadataPredicate> predicates, Collection<ColumnMetadata> columns) {
      return columns.stream()
          .filter(column -> predicates.stream().noneMatch(p -> p.test(column)))
          .toList();
    }
  }

  /**
   * In the `system_schema.indexes` the <code>options</code> field has the extra class_name and
   * target. But in CQL these are not in the <code>WITH OPTIONS</code>
   *
   * <p>Example of <code>system_schema.indexes</code>:
   *
   * <pre>
   * | keyspace_name | table_name | index_name                       | kind   | options                                                                                                                          |
   * |-------------- | ---------- | -------------------------------- | ------ | ---------------------------------------------------------------------------------------------------------------------------------|
   * |     askada_01 |  documents |         documents_array_contains | CUSTOM |                                                       {'class_name': 'StorageAttachedIndex', 'target': 'values(array_contains)'} |
   * |     askada_01 |  documents |             documents_array_size | CUSTOM |                                                          {'class_name': 'StorageAttachedIndex', 'target': 'entries(array_size)'} |
   * |     askada_01 |  documents |            documents_exists_keys | CUSTOM |                                                           {'class_name': 'StorageAttachedIndex', 'target': 'values(exist_keys)'} |
   * |     askada_01 |  documents |      documents_query_bool_values | CUSTOM |                                                   {'class_name': 'StorageAttachedIndex', 'target': 'entries(query_bool_values)'} |
   * |     askada_01 |  documents |       documents_query_dbl_values | CUSTOM |                                                    {'class_name': 'StorageAttachedIndex', 'target': 'entries(query_dbl_values)'} |
   * |     askada_01 |  documents |    documents_query_lexical_value | CUSTOM |                            {'class_name': 'StorageAttachedIndex', 'index_analyzer': 'standard', 'target': 'query_lexical_value'} |
   * |     askada_01 |  documents |      documents_query_null_values | CUSTOM |                                                    {'class_name': 'StorageAttachedIndex', 'target': 'values(query_null_values)'} |
   * |     askada_01 |  documents |      documents_query_text_values | CUSTOM |                                                   {'class_name': 'StorageAttachedIndex', 'target': 'entries(query_text_values)'} |
   * |     askada_01 |  documents | documents_query_timestamp_values | CUSTOM |                                              {'class_name': 'StorageAttachedIndex', 'target': 'entries(query_timestamp_values)'} |
   * |     askada_01 |  documents |     documents_query_vector_value | CUSTOM | {'class_name': 'StorageAttachedIndex', 'similarity_function': 'cosine', 'source_model': 'OTHER', 'target': 'query_vector_value'} |
   * </pre>
   *
   * <p>Example of CQL:
   *
   * <pre>
   * CREATE CUSTOM INDEX IF NOT EXISTS documents_exists_keys ON "keyspace".documents (values(exist_keys)) USING 'StorageAttachedIndex';
   * CREATE CUSTOM INDEX IF NOT EXISTS documents_array_size ON "keyspace".documents (entries(array_size)) USING 'StorageAttachedIndex';
   * CREATE CUSTOM INDEX IF NOT EXISTS documents_array_contains ON "keyspace".documents (values(array_contains)) USING 'StorageAttachedIndex';
   * CREATE CUSTOM INDEX IF NOT EXISTS documents_query_bool_values ON "keyspace".documents (entries(query_bool_values)) USING 'StorageAttachedIndex';
   * CREATE CUSTOM INDEX IF NOT EXISTS documents_query_dbl_values ON "keyspace".documents (entries(query_dbl_values)) USING 'StorageAttachedIndex';
   * CREATE CUSTOM INDEX IF NOT EXISTS documents_query_text_values ON "keyspace".documents (entries(query_text_values)) USING 'StorageAttachedIndex';
   * CREATE CUSTOM INDEX IF NOT EXISTS documents_query_timestamp_values ON "keyspace".documents (entries(query_timestamp_values)) USING 'StorageAttachedIndex';
   * CREATE CUSTOM INDEX IF NOT EXISTS documents_query_null_values ON "keyspace".documents (values(query_null_values)) USING 'StorageAttachedIndex';
   * CREATE CUSTOM INDEX IF NOT EXISTS documents_query_vector_value ON "keyspace".documents (query_vector_value) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'cosine', 'source_model': 'OTHER'};
   * CREATE CUSTOM INDEX IF NOT EXISTS documents_query_lexical_value ON "keyspace".documents (query_lexical_value) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'};
   * </pre>
   *
   * @param columnDef
   * @param indexFunction
   */
  record IndexDef(ColumnDef columnDef, ApiIndexFunction indexFunction) {

    public CqlIdentifier indexName(CqlIdentifier collection) {
      return CqlIdentifier.fromInternal(
          collection.asInternal() + "_" + columnDef.name().asInternal());
    }

    public IndexMetadata indexMetadata(
        CqlIdentifier keyspace, CqlIdentifier collection, Map<String, String> options) {

      // because this is IndexMetadata read from system_schema.indexes
      // we need the options for the `class_name` and `target` AND any other cql "OPTIONS" like
      // vector index config, pass them in
      var indexTarget = new CQLSAIIndex.IndexTarget(columnDef.name, indexFunction);
      Map<String, String> fullOptions =
          options == null ? new LinkedHashMap<>() : new LinkedHashMap<>(options);
      fullOptions.putAll(indexTarget.indexOptions());

      return new DefaultIndexMetadata(
          keyspace,
          collection,
          indexName(collection),
          IndexKind.CUSTOM,
          indexTarget.toTargetString(),
          Collections.unmodifiableMap(fullOptions));
    }

    public static Optional<Map<String, String>> vectorIndexOptions(
        String similarityFunction, String sourceModel) {

      //  {'similarity_function': '${SIMILARITY_FUNCTION}', 'source_model': '${SOURCE_MODEL}'}

      // preserve order, similarity then source model, important for testing against CQL
      Map<String, String> options = new LinkedHashMap<>();
      if (similarityFunction != null && !similarityFunction.isBlank()) {
        options.put(VectorConstants.CQLAnnIndex.SIMILARITY_FUNCTION, similarityFunction);
      }
      if (sourceModel != null && !sourceModel.isBlank()) {
        options.put(VectorConstants.CQLAnnIndex.SOURCE_MODEL, sourceModel);
      }
      return options.isEmpty() ? Optional.empty() : Optional.of(options);
    }

    public static Optional<Map<String, String>> lexicalIndexOptions(String indexAnalyzer) {

      // {'index_analyzer': '${INDEX_ANALYZER}'}
      // preserver order, we only have one, but hey, we preserve order
      Map<String, String> options = new LinkedHashMap<>();
      if (indexAnalyzer != null && !indexAnalyzer.isBlank()) {
        options.put(TableDescConstants.TextIndexCQLOptions.OPTION_ANALYZER, indexAnalyzer);
      }
      return options.isEmpty() ? Optional.empty() : Optional.of(options);
    }

    /**
     * Build the CQL Statement we would use to create this index.
     *
     * <p>
     *
     * @return
     */
    public SimpleStatement statement(
        CqlIdentifier keyspace,
        CqlIdentifier collection,
        boolean ifNotExists,
        Map<String, Object> options) {

      var start =
          SchemaBuilder.createIndex(indexName(collection)).custom(CQLSAIIndex.SAI_CLASS_NAME);
      if (ifNotExists) {
        start = start.ifNotExists();
      }

      var onTable = start.onTable(keyspace, collection);
      var indexTarget = new CQLSAIIndex.IndexTarget(columnDef.name, indexFunction);
      var createIndex = indexTarget.addTo(onTable);

      if (options != null && !options.isEmpty()) {
        // in the CQL statement OPTIONS are the things after WITH, and for the `create index` there
        // is
        // an option called OPTIONS calling withSASIOptions deals with this.
        // NOTE: We use SAI not SASI but all this function does is add an option called "OPTIONS"
        createIndex = createIndex.withSASIOptions(options);
      }

      return new ExtendedCreateIndex((DefaultCreateIndex) createIndex).build();
    }
  }

  interface IndexDefs {

    // Required indexes
    IndexDef EXIST_KEYS = new IndexDef(ColumnDefs.EXIST_KEYS, ApiIndexFunction.VALUES);
    IndexDef ARRAY_SIZE = new IndexDef(ColumnDefs.ARRAY_SIZE, ApiIndexFunction.ENTRIES);
    IndexDef ARRAY_CONTAINS = new IndexDef(ColumnDefs.ARRAY_CONTAINS, ApiIndexFunction.VALUES);
    IndexDef QUERY_BOOLEAN_VALUES =
        new IndexDef(ColumnDefs.QUERY_BOOLEAN_VALUES, ApiIndexFunction.ENTRIES);
    IndexDef QUERY_DOUBLE_VALUES =
        new IndexDef(ColumnDefs.QUERY_DOUBLE_VALUES, ApiIndexFunction.ENTRIES);
    IndexDef QUERY_TEXT_VALUES =
        new IndexDef(ColumnDefs.QUERY_TEXT_VALUES, ApiIndexFunction.ENTRIES);
    IndexDef QUERY_TIMESTAMP_VALUES =
        new IndexDef(ColumnDefs.QUERY_TIMESTAMP_VALUES, ApiIndexFunction.ENTRIES);
    IndexDef QUERY_NULL_VALUES =
        new IndexDef(ColumnDefs.QUERY_NULL_VALUES, ApiIndexFunction.VALUES);
    // Optional indexes
    IndexDef QUERY_VECTOR_VALUE = new IndexDef(ColumnDefs.QUERY_VECTOR_VALUE, null);
    IndexDef QUERY_LEXICAL_VALUE = new IndexDef(ColumnDefs.QUERY_LEXICAL_VALUE, null);

    List<IndexDef> ALL =
        List.of(
            EXIST_KEYS,
            ARRAY_SIZE,
            ARRAY_CONTAINS,
            QUERY_BOOLEAN_VALUES,
            QUERY_DOUBLE_VALUES,
            QUERY_TEXT_VALUES,
            QUERY_TIMESTAMP_VALUES,
            QUERY_NULL_VALUES,
            QUERY_VECTOR_VALUE,
            QUERY_LEXICAL_VALUE);
    List<IndexDef> OPTIONAL = List.of(QUERY_VECTOR_VALUE, QUERY_LEXICAL_VALUE);
    List<IndexDef> REQUIRED = listDifference(ALL, OPTIONAL);

    static List<IndexMetadata> toIndexMetadata(
        CqlIdentifier keyspace,
        CqlIdentifier table,
        List<IndexDef> indexes,
        Map<IndexDef, Map<String, String>> perIndexOptions) {

      Map<IndexDef, Map<String, String>> safeIndexOptions =
          perIndexOptions == null ? Collections.emptyMap() : perIndexOptions;
      return indexes.stream()
          .map(index -> index.indexMetadata(keyspace, table, safeIndexOptions.get(index)))
          .toList();
    }
  }
}
