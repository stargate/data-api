package io.stargate.sgv2.jsonapi.service.provider;

import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import java.util.Optional;

/**
 * If the model usage was for indexing data or searching data
 *
 * <p>Keeps in parity with the grpc proto definition in embedding_gateway.proto.
 */
public enum ModelInputType {

  /**
   * The input type is not specified, for parity with grpc, or where it does not make sense to such
   * as for a reranking model.
   */
  INPUT_TYPE_UNSPECIFIED,
  INDEX,
  SEARCH;

  public static ModelInputType fromEmbeddingRequestType(
      EmbeddingProvider.EmbeddingRequestType embeddingRequestType) {
    return switch (embeddingRequestType) {
      case INDEX -> INDEX;
      case SEARCH -> SEARCH;
    };
  }

  public static Optional<ModelInputType> fromEmbeddingGateway(
      EmbeddingGateway.ModelUsage.InputType inputType) {
    return switch (inputType) {
      case INPUT_TYPE_UNSPECIFIED -> Optional.of(INPUT_TYPE_UNSPECIFIED);
      case INDEX -> Optional.of(INDEX);
      case SEARCH -> Optional.of(SEARCH);
      default -> Optional.empty();
    };
  }

  public EmbeddingGateway.ModelUsage.InputType toEmbeddingGateway() {
    return switch (this) {
      case INPUT_TYPE_UNSPECIFIED -> EmbeddingGateway.ModelUsage.InputType.INPUT_TYPE_UNSPECIFIED;
      case INDEX -> EmbeddingGateway.ModelUsage.InputType.INDEX;
      case SEARCH -> EmbeddingGateway.ModelUsage.InputType.SEARCH;
    };
  }
}
