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
    UPDATE_UNKNOWN_TABLE_COLUMNS,
    UPDATE_PRIMARY_KEY_COLUMNS;

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
