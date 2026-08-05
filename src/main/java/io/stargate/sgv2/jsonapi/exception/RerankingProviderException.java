package io.stargate.sgv2.jsonapi.exception;

import java.util.EnumSet;
import java.util.Optional;
import java.util.UUID;

public class RerankingProviderException extends ServerException {

  public static final Scope SCOPE = Scope.RERANKING_PROVIDER;

  public RerankingProviderException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  /** Constructs a reranking provider exception from an unrecognized EGW error response. */
  public RerankingProviderException(String code, String title, String body) {
    this(
        new ErrorInstance(
            UUID.randomUUID(),
            FAMILY,
            SCOPE,
            code,
            title,
            body,
            Optional.empty(),
            EnumSet.noneOf(ExceptionFlags.class)));
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
