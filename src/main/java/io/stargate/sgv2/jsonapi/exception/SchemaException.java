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
    UNKNOWN_PARTITION_SORT_COLUMNS,
    ZERO_PARTITION_COLUMNS,
    UNKNOWN_PARTITION_COLUMNS,
    COLUMN_ALREADY_EXISTS,
    CANNOT_DROP_PRIMARY_KEY_COLUMNS,
    CANNOT_DROP_UNKNOWN_COLUMNS,
    CANNOT_DROP_INDEXED_COLUMNS,
    CANNOT_VECTORIZE_UNKNOWN_COLUMNS,
    CANNOT_VECTORIZE_NON_VECTOR_COLUMNS,
    CANNOT_DROP_VECTORIZE_FROM_UNKNOWN_COLUMNS,
    CANNOT_DROP_VECTORIZE_FROM_NON_VECTOR_COLUMNS,

    COLUMN_NOT_FOUND,
    // COLUMN_CANNOT_BE_DROPPED,
    COLUMN_TYPE_INCORRECT,
    COLUMN_TYPE_UNSUPPORTED,
    INVALID_CONFIGURATION,
    INVALID_INDEX_DEFINITION,
    INVALID_KEYSPACE,
    INVALID_VECTORIZE_CONFIGURATION,
    LIST_TYPE_INVALID_DEFINITION,
    MAP_TYPE_INVALID_DEFINITION,
    MISSING_PRIMARY_KEYS,
    //    NON_VECTOR_TYPE_COLUMN,
    //    PRIMARY_KEY_DEFINITION_INCORRECT,
    SET_TYPE_INVALID_DEFINITION,
    VECTOR_TYPE_INVALID_DEFINITION;

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
