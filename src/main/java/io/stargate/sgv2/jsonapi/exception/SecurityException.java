package io.stargate.sgv2.jsonapi.exception;

/**
 * Errors related to the security aspects of a request.
 *
 * <p>See {@link APIException} for steps to add a new code.
 */
public class SecurityException extends RequestException {

  public static final Scope SCOPE = Scope.SECURITY;

  public SecurityException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<SecurityException> {
    UNAUTHENTICATED_REQUEST,
    UNAUTHORIZED_ACCESS;

    private final ErrorTemplate<SecurityException> template;

    Code() {
      template = ErrorTemplate.load(SecurityException.class, FAMILY, SCOPE, name());
    }

    @Override
    public ErrorTemplate<SecurityException> template() {
      return template;
    }
  }
}
