package io.stargate.sgv2.jsonapi.exception;

public class ProviderException extends ServerException {

  public static final Scope SCOPE = Scope.PROVIDER;

  public ProviderException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<ProviderException> {
    CLIENT_ERROR,
    SERVER_ERROR,
    TIMEOUT,
    TOO_MANY_REQUESTS,
    UNEXPECTED_RESPONSE;

    private final ErrorTemplate<ProviderException> template;

    Code() {
      template = ErrorTemplate.load(ProviderException.class, FAMILY, SCOPE, name());
    }

    @Override
    public ErrorTemplate<ProviderException> template() {
      return template;
    }
  }
}
