package io.stargate.sgv2.jsonapi.exception;

/**
 * Errors related to the schema definition in a request.
 *
 * <p>See {@link APIException} for steps to add a new code.
 */
public class SchemaException extends RequestException {

  public static final Scope SCOPE = Scope.SCHEMA;

  public SchemaException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<SchemaException> {
    CANNOT_ADD_EXISTING_COLUMNS,
    CANNOT_ADD_EXISTING_INDEX,
    CANNOT_ADD_EXISTING_TABLE,
    CANNOT_DROP_INDEXED_COLUMNS,
    CANNOT_DROP_PRIMARY_KEY_COLUMNS,
    CANNOT_DROP_UNKNOWN_COLUMNS,
    CANNOT_DROP_UNKNOWN_INDEX,
    CANNOT_DROP_UNKNOWN_TABLE,
    CANNOT_DROP_VECTORIZE_FROM_NON_VECTOR_COLUMNS,
    CANNOT_DROP_VECTORIZE_FROM_UNKNOWN_COLUMNS,
    CANNOT_VECTORIZE_NON_VECTOR_COLUMNS,
    CANNOT_VECTORIZE_UNKNOWN_COLUMNS,
    MISSING_PARTITION_COLUMNS,
    UNKNOWN_DATA_TYPE,
    UNKNOWN_INDEX_COLUMN,
    UNKNOWN_PARTITION_COLUMNS,
    UNKNOWN_PARTITION_SORT_COLUMNS,
    UNKNOWN_PRIMITIVE_DATA_TYPE,
    UNKNOWN_VECTOR_METRIC,
    UNKNOWN_VECTOR_SOURCE_MODEL,
    UNSUPPORTED_INDEXING_FOR_DATA_TYPES,
    UNSUPPORTED_LIST_DEFINITION,
    UNSUPPORTED_MAP_DEFINITION,
    UNSUPPORTED_SET_DEFINITION,
    UNSUPPORTED_TEXT_ANALYSIS_FOR_DATA_TYPES,
    UNSUPPORTED_VECTOR_DIMENSION,
    UNSUPPORTED_VECTOR_INDEX_FOR_DATA_TYPES,
    UNSUPPORTED_DIFFERENT_EMBEDDING_SERVICE_CONFIGS,

    // older below - seperated because they need to be confirmed
    INVALID_CONFIGURATION,
    INVALID_KEYSPACE,
    INVALID_VECTORIZE_CONFIGURATION;

    private final ErrorTemplate<SchemaException> template;

    Code() {
      template = ErrorTemplate.load(SchemaException.class, FAMILY, SCOPE, name());
    }

    @Override
    public ErrorTemplate<SchemaException> template() {
      return template;
    }
  }
}
