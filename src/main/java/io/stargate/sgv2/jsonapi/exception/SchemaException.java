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
    COLUMN_DEFINITION_MISSING,
    COLUMN_TYPE_INCORRECT,
    COLUMN_TYPE_UNSUPPORTED,
    INVALID_CONFIGURATION,
    INVALID_INDEX_DEFINITION,
    INVALID_VECTORIZE_CONFIGURATION,
    LIST_TYPE_INCORRECT_DEFINITION,
    MAP_TYPE_INCORRECT_DEFINITION,
    MISSING_PRIMARY_KEYS,
    PRIMARY_KEY_DEFINITION_INCORRECT,
    SET_TYPE_INCORRECT_DEFINITION,
    VECTOR_TYPE_INCORRECT_DEFINITION;

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
