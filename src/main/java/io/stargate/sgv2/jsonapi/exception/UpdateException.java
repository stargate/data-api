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
    UNSUPPORTED_OVERLAPPING_UPDATE_OPERATIONS,
    MISSING_UPDATE_OPERATIONS,
    INVALID_UPDATE_COLUMN_VALUES,
    UNKNOWN_TABLE_COLUMNS,
    UPDATE_OPERATOR_PULL_ALL_REQUIRES_ARRAY_VALUE,
    INVALID_USAGE_FOR_COLLECTION_ONLY_UPDATE_OPERATORS,
    INVALID_USAGE_OF_PUSH_OPERATOR,
    UNSUPPORTED_UPDATE_FOR_PRIMARY_KEY_COLUMNS,
    UNSUPPORTED_UPDATE_OPERATIONS_FOR_TABLE,
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
