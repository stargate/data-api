package io.stargate.sgv2.jsonapi.exception;

/**
 * Errors related to the security aspects of a request.
 *
 * <p>See {@link APIException} for steps to add a new code.
 *
 * <p>NOTE: Class is called APISecurityException to avoid confusion with existing SecurityException
 * in Java
 */
public class APISecurityException extends RequestException {

  public static final Scope SCOPE = Scope.SECURITY;

  public APISecurityException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<APISecurityException> {
    MISSING_AUTHENTICATION_TOKEN,
    UNAUTHENTICATED_REQUEST,
    UNAUTHORIZED_ACCESS;

    private final ErrorTemplate<APISecurityException> template;

    Code() {
      template = ErrorTemplate.load(APISecurityException.class, FAMILY, SCOPE, name());
    }

    @Override
    public ErrorTemplate<APISecurityException> template() {
      return template;
    }
  }
}
