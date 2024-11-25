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
    DUPLICATE_UPDATE_OPERATION_ASSIGNMENTS,
    MISSING_UPDATE_OPERATIONS,
    UNKNOWN_TABLE_COLUMNS,
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
