package io.stargate.sgv2.jsonapi.exception.playing;

public class EmbeddingProviderException extends ServerException {
  public EmbeddingProviderException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<EmbeddingProviderException> {
    CLIENT_ERROR,
    SERVER_ERROR;

    private final ErrorTemplate<EmbeddingProviderException> template;

    Code() {
      template =
          ErrorTemplate.load(
              EmbeddingProviderException.class,
              ErrorFamily.SERVER,
              Scope.EMBEDDING_PROVIDER,
              name());
    }

    @Override
    public ErrorTemplate<EmbeddingProviderException> template() {
      return template;
    }
  }
}
