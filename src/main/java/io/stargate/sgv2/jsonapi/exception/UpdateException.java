package io.stargate.sgv2.jsonapi.exception;

/**
 * Errors related to the filter clause in a request.
 *
 * <p>See {@link APIException} for steps to add a new code.
 */
public class UpdateException extends RequestException {

  public static final Scope SCOPE = Scope.UPDATE;

  public UpdateException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<UpdateException> {
    INVALID_PUSH_OPERATOR_USAGE,
    INVALID_UPDATE_COLUMN_VALUES,
    MISSING_UPDATE_OPERATIONS,
    UNKNOWN_TABLE_COLUMNS,
    UNSUPPORTED_COLUMN_TYPES,
    UNSUPPORTED_OVERLAPPING_UPDATE_OPERATIONS,
    UNSUPPORTED_UPDATE_FOR_PRIMARY_KEY_COLUMNS,
    UNSUPPORTED_UPDATE_OPERATIONS_FOR_TABLE,
    UNSUPPORTED_UPDATE_OPERATOR,
    UNSUPPORTED_UPDATE_OPERATOR_FOR_DOC_ID,
    UNSUPPORTED_UPDATE_OPERATOR_FOR_LEXICAL,
    UNSUPPORTED_UPDATE_OPERATOR_FOR_VECTOR,
    UNSUPPORTED_UPDATE_OPERATOR_FOR_VECTORIZE,
    UNSUPPORTED_VECTORIZE_WHEN_MISSING_VECTORIZE_DEFINITION;

    private final ErrorTemplate<UpdateException> template;

    Code() {
      template = ErrorTemplate.load(UpdateException.class, FAMILY, SCOPE, name());
    }

    @Override
    public ErrorTemplate<UpdateException> template() {
      return template;
    }
  }
}
