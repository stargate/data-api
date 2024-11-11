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
    // REMOVE !
    INVALID_INDEX_DEFINITION,

    CANNOT_DROP_INDEXED_COLUMNS,
    CANNOT_DROP_PRIMARY_KEY_COLUMNS,
    CANNOT_DROP_UNKNOWN_COLUMNS,
    CANNOT_DROP_VECTORIZE_FROM_NON_VECTOR_COLUMNS,
    CANNOT_DROP_VECTORIZE_FROM_UNKNOWN_COLUMNS,
    CANNOT_VECTORIZE_NON_VECTOR_COLUMNS,
    CANNOT_VECTORIZE_UNKNOWN_COLUMNS,
    COLUMN_ALREADY_EXISTS,
    DATA_TYPE_NOT_SUPPORTED_BY_INDEXING,
    TEXT_ANALYSIS_NOT_SUPPORTED_BY_DATA_TYPE,
    UNKNOWN_DATA_TYPE,
    UNKNOWN_INDEX_COLUMN,
    UNKNOWN_PARTITION_COLUMNS,
    UNKNOWN_PARTITION_SORT_COLUMNS,
    UNKNOWN_PRIMITIVE_DATA_TYPE,
    UNKNOWN_VECTOR_METRIC,
    UNKNOWN_VECTOR_SOURCE_MODEL,
    UNSUPPORTED_LIST_DEFINITION,
    UNSUPPORTED_MAP_DEFINITION,
    UNSUPPORTED_SET_DEFINITION,
    UNSUPPORTED_VECTOR_DIMENSION,
    VECTOR_INDEX_NOT_SUPPORTED_BY_DATA_TYPE,
    ZERO_PARTITION_COLUMNS,

    // older below - seperated because they need to be confirmed
    //    COLUMN_TYPE_INCORRECT,
    INDEX_NOT_FOUND,
    INVALID_CONFIGURATION,
    // INVALID_INDEX_DEFINITION,
    INVALID_KEYSPACE,
    INVALID_VECTORIZE_CONFIGURATION,
    TABLE_ALREADY_EXISTS,
    TABLE_NOT_FOUND,
    ;

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
