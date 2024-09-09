package io.stargate.sgv2.jsonapi.exception;

/**
 * Errors related to the authentication in a request.
 *
 * <p>See {@link APIException} for steps to add a new code.
 */
public class AuthException extends RequestException {

  public static final Scope SCOPE = Scope.AUTHENTICATION;

  public AuthException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<AuthException> {
    INVALID_TOKEN;

    private final ErrorTemplate<AuthException> template;

    Code() {
      template = ErrorTemplate.load(AuthException.class, FAMILY, SCOPE, name());
    }

    @Override
    public ErrorTemplate<AuthException> template() {
      return template;
    }
  }
}
