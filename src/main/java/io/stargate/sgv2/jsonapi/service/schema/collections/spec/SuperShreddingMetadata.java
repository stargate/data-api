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
import java.util.stream.Stream;import static io.stargate.sgv2.jsonapi.util.StringUtil.isNullOrBlank;

/**
 * Canonical definition of the structure of a super-shredding table,
 * that is used in production to make super-shredding tables and test their behavior.
 * <p>
 * <b>NOTE:</b> please keep the columns and indexes in order. We have also <b>excluded</b>
 * this class from formatting so we can format for ease of reading. This file makes
 * more sense when read top to bottom, as it builds up the ideas.
 * </p>
 * <p>
 * The objects created by {@link SuperShreddingBuilder} 's using this information is then
 * tested against CQL from {@link SuperShreddingCQLBuilder}, see the builder and
 * <code>SuperShreddingBuilderTest</code> for how we build up the tests.
 * </p>
 */
public interface SuperShreddingMetadata {

  static <T> List<T> listDifference(List<T> list1, List<T> list2) {
    return list1.stream().filter(item -> !list2.contains(item)).collect(Collectors.toList());
  }

  /**
   * String names of all columns, in the order that we traditionally have them in the collection
   * table, pls try to keep the order :)
   * Use the {@link Identifiers} if you want {@link CqlIdentifier}s.
   */
  interface Names {

    // Required columns
    String KEY                      = "key";
    String TX_ID                    = "tx_id";
    String DOC_JSON                 = "doc_json";
    String EXIST_KEYS               = "exist_keys";
    String ARRAY_SIZE               = "array_size";
    String ARRAY_CONTAINS           = "array_contains";
    String QUERY_BOOLEAN_VALUES     = "query_bool_values";
    String QUERY_DOUBLE_VALUES      = "query_dbl_values";
    String QUERY_TEXT_VALUES        = "query_text_values"; // old comment > Text map support _id $ne and _id $nin on both atomic value and array element
    String QUERY_TIMESTAMP_VALUES   = "query_timestamp_values";
    String QUERY_NULL_VALUES        = "query_null_values";
    // Optional columns
    String QUERY_VECTOR_VALUE       = "query_vector_value";
    String QUERY_LEXICAL_VALUE      = "query_lexical_value";

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
    List<String> OPTIONAL = List.of(QUERY_VECTOR_VALUE, QUERY_LEXICAL_VALUE);
    List<String> REQUIRED = listDifference(ALL, OPTIONAL);
    List<String> REQUIRED_NON_PK = listDifference(REQUIRED, PARTITION_KEY);
  }

  /**
   * {@link CqlIdentifier}s of all columns, in the order that we traditionally have them in
   * the collection table, pls try to keep the order :)
   */
  interface Identifiers {

    // Required columns
    CqlIdentifier KEY                    = CqlIdentifier.fromInternal(Names.KEY);
    CqlIdentifier TX_ID                  = CqlIdentifier.fromInternal(Names.TX_ID);
    CqlIdentifier DOC_JSON               = CqlIdentifier.fromInternal(Names.DOC_JSON);
    CqlIdentifier EXIST_KEYS             = CqlIdentifier.fromInternal(Names.EXIST_KEYS);
    CqlIdentifier ARRAY_SIZE             = CqlIdentifier.fromInternal(Names.ARRAY_SIZE);
    CqlIdentifier ARRAY_CONTAINS         = CqlIdentifier.fromInternal(Names.ARRAY_CONTAINS);
    CqlIdentifier QUERY_BOOLEAN_VALUES   = CqlIdentifier.fromInternal(Names.QUERY_BOOLEAN_VALUES);
    CqlIdentifier QUERY_DOUBLE_VALUES    = CqlIdentifier.fromInternal(Names.QUERY_DOUBLE_VALUES);
    CqlIdentifier QUERY_TEXT_VALUES      = CqlIdentifier.fromInternal(Names.QUERY_TEXT_VALUES);
    CqlIdentifier QUERY_TIMESTAMP_VALUES = CqlIdentifier.fromInternal(Names.QUERY_TIMESTAMP_VALUES);
    CqlIdentifier QUERY_NULL_VALUES      = CqlIdentifier.fromInternal(Names.QUERY_NULL_VALUES);
    // Optional columns
    CqlIdentifier QUERY_VECTOR_VALUE     = CqlIdentifier.fromInternal(Names.QUERY_VECTOR_VALUE);
    CqlIdentifier QUERY_LEXICAL_VALUE    = CqlIdentifier.fromInternal(Names.QUERY_LEXICAL_VALUE);

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
    List<CqlIdentifier> OPTIONAL = List.of(QUERY_VECTOR_VALUE, QUERY_LEXICAL_VALUE);
    List<CqlIdentifier> REQUIRED = listDifference(ALL, OPTIONAL);
    List<CqlIdentifier> REQUIRED_NON_PK = listDifference(REQUIRED, PARTITION_KEY);
  }

  /**
   * Function for creating the column metadata for a column, only needed with the vector becase
   * we dont know all the info for the column until it is bound to a definition
   */
  @FunctionalInterface
  interface ColumnMetadataFactory {
    ColumnMetadata columnMetadata(ColumnDef columnDef, SuperShreddingBinding binding);
  }

  /**
   * A definition of a column in a super shredding table, which can then be bound to a
   * super shredding definition to create the ColumnMetadata and schema statements we need
   * to create a particular table.
   * <p>
   *  The properties of the record define the general case of a column in super shredding, the methods
   *  allow objects to be created for the specific case of a specific table.
   * </p>
   */
  record ColumnDef(CqlIdentifier name, DataType type, ColumnMetadataFactory metadataFactory) {

    ColumnDef(CqlIdentifier name, DataType type) {
      this(name, type, null);
    }

    public ColumnMetadata columnMetadata(SuperShreddingBinding binding) {
      if (metadataFactory == null) {
        return new DefaultColumnMetadata(binding.keyspace(), binding.collection(), name, type, false);
      }
      var factoryValue = metadataFactory.columnMetadata(this, binding);
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

  /**
   * The list of {@link ColumnDef} for all the columns in a super shredding table.
   * <p>
   *  Use the {@link SuperShreddingMetadataBuilder} to build TableMetadata and IndexMetadata,
   *  use the XXX (TODO:) builder to create statements.
   * </p>
   */
  interface ColumnDefs {

    // Required columns
    ColumnDef KEY                     = new ColumnDef(Identifiers.KEY,                    DataTypes.tupleOf(DataTypes.TINYINT, DataTypes.TEXT));
    ColumnDef TX_ID                   = new ColumnDef(Identifiers.TX_ID,                  DataTypes.TIMEUUID);
    ColumnDef DOC_JSON                = new ColumnDef(Identifiers.DOC_JSON,               DataTypes.TEXT);
    ColumnDef EXIST_KEYS              = new ColumnDef(Identifiers.EXIST_KEYS,             DataTypes.setOf(DataTypes.TEXT));
    ColumnDef ARRAY_SIZE              = new ColumnDef(Identifiers.ARRAY_SIZE,             DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT));
    ColumnDef ARRAY_CONTAINS          = new ColumnDef(Identifiers.ARRAY_CONTAINS,         DataTypes.setOf(DataTypes.TEXT));
    ColumnDef QUERY_BOOLEAN_VALUES    = new ColumnDef(Identifiers.QUERY_BOOLEAN_VALUES,   DataTypes.mapOf(DataTypes.TEXT, DataTypes.TINYINT));
    ColumnDef QUERY_DOUBLE_VALUES     = new ColumnDef(Identifiers.QUERY_DOUBLE_VALUES,    DataTypes.mapOf(DataTypes.TEXT, DataTypes.DECIMAL));
    ColumnDef QUERY_TEXT_VALUES       = new ColumnDef(Identifiers.QUERY_TEXT_VALUES,      DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT));
    ColumnDef QUERY_TIMESTAMP_VALUES  = new ColumnDef(Identifiers.QUERY_TIMESTAMP_VALUES, DataTypes.mapOf(DataTypes.TEXT, DataTypes.TIMESTAMP));
    ColumnDef QUERY_NULL_VALUES       = new ColumnDef(Identifiers.QUERY_NULL_VALUES,      DataTypes.setOf(DataTypes.TEXT));

    // Optional columns
    // NOTE: using our extended vector, length is dependent on the vector dimension of the
    // collection
    ColumnDef QUERY_VECTOR_VALUE =  new ColumnDef(Identifiers.QUERY_VECTOR_VALUE,         new ExtendedVectorType(DataTypes.FLOAT, 1),   ColumnDefs::vectorColumnMetadataFactory);
    ColumnDef QUERY_LEXICAL_VALUE = new ColumnDef(Identifiers.QUERY_LEXICAL_VALUE,        DataTypes.TEXT);

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
    List<ColumnDef> OPTIONAL = List.of(QUERY_VECTOR_VALUE, QUERY_LEXICAL_VALUE);
    List<ColumnDef> REQUIRED = listDifference(ALL, OPTIONAL);
    List<ColumnDef> REQUIRED_NON_PK = listDifference(REQUIRED, PARTITION_KEY);

    static ColumnMetadata vectorColumnMetadataFactory(ColumnDef columnDef, SuperShreddingBinding binding){

      if (!binding.isVectorDefined()) {
        throw new IllegalArgumentException("SuperShreddingBinding does not define the vector column, binding: %s".formatted(binding));
      }
      var elementType = ((ExtendedVectorType) ColumnDefs.QUERY_VECTOR_VALUE.type()).getElementType();
      var vectorWithDimension = new ExtendedVectorType(elementType, binding.vectorLength());

      return new DefaultColumnMetadata(
              binding.keyspace(),
              binding.collection(),
              columnDef.name(),
              vectorWithDimension,
              false);
    }

    static Stream<ColumnMetadata> toColumnMetadata(
        List<ColumnDef> columnDefs,
        SuperShreddingBinding binding) {

      Objects.requireNonNull(binding, "binding must not be null");
      return columnDefs.stream()
          .map(columnDef -> columnDef.columnMetadata(binding));
    }
  }

  /**
   * Predicates that can be used to test if a ColumnMetadata matches the definition for a
   * super shredding column. Use the {@link SuperShreddingPredicateBuilder} to get a
   * predciate that can match a specific {@link SuperShreddingBinding}
   *
   */
  interface Predicates {

    // Required columns
    ColumnMetadataPredicate KEY                    = ColumnDefs.KEY.predicate();
    ColumnMetadataPredicate TX_ID                  = ColumnDefs.TX_ID.predicate();
    ColumnMetadataPredicate DOC_JSON               = ColumnDefs.DOC_JSON.predicate();
    ColumnMetadataPredicate EXIST_KEYS             = ColumnDefs.EXIST_KEYS.predicate();
    ColumnMetadataPredicate ARRAY_SIZE             = ColumnDefs.ARRAY_SIZE.predicate();
    ColumnMetadataPredicate ARRAY_CONTAINS         = ColumnDefs.ARRAY_CONTAINS.predicate();
    ColumnMetadataPredicate QUERY_BOOLEAN_VALUES   = ColumnDefs.QUERY_BOOLEAN_VALUES.predicate();
    ColumnMetadataPredicate QUERY_DOUBLE_VALUES    = ColumnDefs.QUERY_DOUBLE_VALUES.predicate();
    ColumnMetadataPredicate QUERY_TEXT_VALUES      = ColumnDefs.QUERY_TEXT_VALUES.predicate();
    ColumnMetadataPredicate QUERY_TIMESTAMP_VALUES = ColumnDefs.QUERY_TIMESTAMP_VALUES.predicate();
    ColumnMetadataPredicate QUERY_NULL_VALUES      = ColumnDefs.QUERY_NULL_VALUES.predicate();
    // Optional columns
    // NOTE: using our extended vector, length is dependent on the vector dimension of the collection
    ColumnMetadataPredicate QUERY_VECTOR_VALUE     = new ColumnMetadataPredicate.Vector(
            ColumnDefs.QUERY_VECTOR_VALUE.name(),
            ((ExtendedVectorType) ColumnDefs.QUERY_VECTOR_VALUE.type()).getElementType());
    ColumnMetadataPredicate QUERY_LEXICAL_VALUE    = ColumnDefs.QUERY_LEXICAL_VALUE.predicate();

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
    List<ColumnMetadataPredicate> REQUIRED_NON_PK = listDifference(REQUIRED, PARTITION_KEY);

    /**
     * Find all the predicates that do not have any matching columns to find columns that we
     * expect to be there but are missing.
     */
    static List<ColumnMetadataPredicate> allFailingPredicates(
        List<ColumnMetadataPredicate> predicates, Collection<ColumnMetadata> columns) {
      return predicates.stream()
          .filter(predicate -> columns.stream().noneMatch(predicate))
          .toList();
    }

    /**
     * Get the list of columns that do not match any of the supplied predicates, to find the
     * columns we do not expect to see.
     */
    static List<ColumnMetadata> allUnexpectedColumns(
        List<ColumnMetadataPredicate> predicates, Collection<ColumnMetadata> columns) {
      return columns.stream()
          .filter(column -> predicates.stream().noneMatch(p -> p.test(column)))
          .toList();
    }
  }

  /**
   * Function used with the {@link IndexDef} to support extra options from the
   * binding for use with the index for creating metadata or create statements
   */
  @FunctionalInterface
  interface IndexOptionsFactory{
    /**
     * @return Options to apply, must not be null
     */
    Map<String, String> apply(SuperShreddingBinding binding);
  }


  /**
   * Models an index on a column in a super shredding table, and the function that is used
   * with the index, e.g. `entries` or `values`.
   * <p>
   * The below information is reference info for what it looks like when we are creating
   * fake TableMetadata (which is built from system_schema.indexes) and when we
   * make a <code>CREATE INDEX</code> statement..
   * <p>
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
   */
  record IndexDef(ColumnDef columnDef, ApiIndexFunction indexFunction, IndexOptionsFactory optionsFactory) {

    public IndexDef(ColumnDef columnDef, ApiIndexFunction indexFunction){
      this(columnDef, indexFunction, null);
    }

    /**
     * Get the name to give this index when bound to the SuperShreddingBinding.
     * <p>
     *  e.g. if the collection is called <code>users</code>, the index on
     *  <code>exist_keys</code> column is called <code>users_exist_keys</code>.
     */
    public CqlIdentifier indexName(SuperShreddingBinding binding) {
      return CqlIdentifier.fromInternal(
              binding.collection().asInternal() + "_" + columnDef.name().asInternal());
    }

    /**
     * Builds {@link IndexMetadata} for this index for the given {@link SuperShreddingBinding},
     * see the {@link SuperShreddingMetadataBuilder} for how this it made with the table metadata.
     */
    public IndexMetadata indexMetadata(SuperShreddingBinding binding) {

      // because this is IndexMetadata read from system_schema.indexes
      // we need the options for the `class_name` and `target` AND any other cql "OPTIONS" like
      // the vector index configuration
      var indexTarget = new CQLSAIIndex.IndexTarget(columnDef.name, indexFunction);
      Map<String, String> fullOptions = new LinkedHashMap<>(indexTarget.indexOptions());

      // any per index options
      fullOptions.putAll(indexOptions(binding));

      return new DefaultIndexMetadata(
          binding.keyspace(),
          binding.collection(),
          indexName(binding),
          IndexKind.CUSTOM,
          indexTarget.toTargetString(),
          Collections.unmodifiableMap(fullOptions));
    }

    Map<String, String> indexOptions(SuperShreddingBinding binding) {
      if (optionsFactory == null) {
        return Collections.emptyMap();
      }
      return optionsFactory.apply(binding);
    }

  }

  interface IndexDefs {

    // Required indexes
    IndexDef EXIST_KEYS             = new IndexDef(ColumnDefs.EXIST_KEYS,             ApiIndexFunction.VALUES);
    IndexDef ARRAY_SIZE             = new IndexDef(ColumnDefs.ARRAY_SIZE,             ApiIndexFunction.ENTRIES);
    IndexDef ARRAY_CONTAINS         = new IndexDef(ColumnDefs.ARRAY_CONTAINS,         ApiIndexFunction.VALUES);
    IndexDef QUERY_BOOLEAN_VALUES   = new IndexDef(ColumnDefs.QUERY_BOOLEAN_VALUES,   ApiIndexFunction.ENTRIES);
    IndexDef QUERY_DOUBLE_VALUES    = new IndexDef(ColumnDefs.QUERY_DOUBLE_VALUES,    ApiIndexFunction.ENTRIES);
    IndexDef QUERY_TEXT_VALUES      = new IndexDef(ColumnDefs.QUERY_TEXT_VALUES,      ApiIndexFunction.ENTRIES);
    IndexDef QUERY_TIMESTAMP_VALUES = new IndexDef(ColumnDefs.QUERY_TIMESTAMP_VALUES, ApiIndexFunction.ENTRIES);
    IndexDef QUERY_NULL_VALUES      = new IndexDef(ColumnDefs.QUERY_NULL_VALUES,      ApiIndexFunction.VALUES);
    // Optional indexes
    IndexDef QUERY_VECTOR_VALUE     = new IndexDef(ColumnDefs.QUERY_VECTOR_VALUE,     null, IndexDefs::vectorIndexOptionsFactory);
    IndexDef QUERY_LEXICAL_VALUE    = new IndexDef(ColumnDefs.QUERY_LEXICAL_VALUE,    null, IndexDefs::lexicalIndexOptionsFactory);

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

    static Map<String, String> vectorIndexOptionsFactory(SuperShreddingBinding binding) {

      //  {'similarity_function': '${SIMILARITY_FUNCTION}', 'source_model': '${SOURCE_MODEL}'}

      // preserve order, similarity then source model, important for testing against CQL
      Map<String, String> options = new LinkedHashMap<>();
      if (!isNullOrBlank(binding.similarityFunction())) {
        options.put(VectorConstants.CQLAnnIndex.SIMILARITY_FUNCTION, binding.similarityFunction());
      }
      if (!isNullOrBlank(binding.sourceModel())) {
        options.put(VectorConstants.CQLAnnIndex.SOURCE_MODEL, binding.sourceModel());
      }
      return options;
    }

    static Map<String, String> lexicalIndexOptionsFactory(SuperShreddingBinding binding) {

      // {'index_analyzer': '${INDEX_ANALYZER}'}
      // preserver order, we only have one, but hey, we preserve order
      Map<String, String> options = new LinkedHashMap<>();
      if (!isNullOrBlank(binding.indexAnalyzer())){
        options.put(TableDescConstants.TextIndexCQLOptions.OPTION_ANALYZER, binding.indexAnalyzer());
      }
      return options;
    }

  }
}
