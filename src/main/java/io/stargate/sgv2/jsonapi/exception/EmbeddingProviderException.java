package io.stargate.sgv2.jsonapi.exception;

public class EmbeddingProviderException extends ServerException {

  public static final Scope SCOPE = Scope.EMBEDDING_PROVIDER;

  public EmbeddingProviderException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<EmbeddingProviderException> {
    EMBEDDING_REQUEST_ENCODING_ERROR,
    EMBEDDING_RESPONSE_DECODING_ERROR,
    EMBEDDING_PROVIDER_AUTHENTICATION_KEYS_NOT_PROVIDED,
    EMBEDDING_PROVIDER_CLIENT_ERROR,
    EMBEDDING_PROVIDER_RATE_LIMITED,
    EMBEDDING_PROVIDER_SERVER_ERROR;

    private final ErrorTemplate<EmbeddingProviderException> template;

    Code() {
      template = ErrorTemplate.load(EmbeddingProviderException.class, FAMILY, SCOPE, name());
    }

    @Override
    public ErrorTemplate<EmbeddingProviderException> template() {
      return template;
    }
  }
}
