package io.stargate.sgv2.jsonapi.exception;

import java.util.EnumSet;
import java.util.Optional;
import java.util.UUID;

public class EmbeddingProviderException extends ServerException {

  public static final Scope SCOPE = Scope.EMBEDDING_PROVIDER;

  public EmbeddingProviderException(ErrorInstance errorInstance) {
    super(errorInstance);
  }

  /**
   * To construct an EmbeddingProviderException from EGW response.
   *
   * @see io.stargate.sgv2.jsonapi.service.embedding.gateway.EmbeddingGatewayClient
   */
  public EmbeddingProviderException(String code, String title, String body) {
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

  public enum Code implements ErrorCode<EmbeddingProviderException> {
    EMBEDDING_GATEWAY_NOT_AVAILABLE,
    EMBEDDING_GATEWAY_REQUEST_PROCESSING_ERROR,
    EMBEDDING_GATEWAY_RATE_LIMITED,

    EMBEDDING_REQUEST_ENCODING_ERROR,
    EMBEDDING_RESPONSE_DECODING_ERROR,
    EMBEDDING_PROVIDER_AUTHENTICATION_KEYS_NOT_PROVIDED,
    EMBEDDING_PROVIDER_CLIENT_ERROR,
    EMBEDDING_PROVIDER_RATE_LIMITED,
    EMBEDDING_PROVIDER_SERVER_ERROR,
    EMBEDDING_PROVIDER_TIMEOUT,
    EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE,
    ;

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
