package io.stargate.sgv2.jsonapi.exception;

public class RerankingProviderException extends ServerException {

  public static final Scope SCOPE = Scope.RERANKING_PROVIDER;

  public RerankingProviderException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  public enum Code implements ErrorCode<RerankingProviderException> {
    RERANKING_PROVIDER_TIMEOUT;

    private final ErrorTemplate<RerankingProviderException> template;

    Code() {
      template = ErrorTemplate.load(RerankingProviderException.class, FAMILY, SCOPE, name());
    }

    @Override
    public ErrorTemplate<RerankingProviderException> template() {
      return template;
    }
  }
}
