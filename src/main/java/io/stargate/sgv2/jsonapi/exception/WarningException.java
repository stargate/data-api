package io.stargate.sgv2.jsonapi.exception;

/**
 * Warning as RequestException for collections or tables.
 *
 * <p>See {@link APIException} for steps to add a new code.
 */
public class WarningException extends RequestException {

  public static final Scope SCOPE = Scope.WARNING;

  public WarningException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<WarningException> {
    ALLOW_FILTERING;

    private final ErrorTemplate<WarningException> template;

    Code() {
      template = ErrorTemplate.load(WarningException.class, FAMILY, SCOPE, name());
    }

    @Override
    public ErrorTemplate<WarningException> template() {
      return template;
    }
  }
}
