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
    COLUMN_ALREADY_EXISTS,
    COLUMN_DEFINITION_MISSING,
    COLUMN_NOT_FOUND,
    COLUMN_CANNOT_BE_DROPPED,
    COLUMN_TYPE_INCORRECT,
    COLUMN_TYPE_UNSUPPORTED,
    INDEX_NOT_FOUND,
    INVALID_CONFIGURATION,
    INVALID_INDEX_DEFINITION,
    INVALID_KEYSPACE,
    INVALID_VECTORIZE_CONFIGURATION,
    LIST_TYPE_INVALID_DEFINITION,
    MAP_TYPE_INVALID_DEFINITION,
    MISSING_PRIMARY_KEYS,
    NON_VECTOR_TYPE_COLUMN,
    PRIMARY_KEY_DEFINITION_INCORRECT,
    SET_TYPE_INVALID_DEFINITION,
    TABLE_ALREADY_EXISTS,
    TABLE_NOT_FOUND,
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
