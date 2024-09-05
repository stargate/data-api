package io.stargate.sgv2.jsonapi.exception;

public class EmbeddingProviderException extends ServerException {

  public static final Scope SCOPE = Scope.EMBEDDING_PROVIDER;

  public EmbeddingProviderException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<EmbeddingProviderException> {
    CLIENT_ERROR,
    SERVER_ERROR;

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
