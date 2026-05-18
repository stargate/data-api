package io.stargate.sgv2.jsonapi.service.provider;

import io.stargate.embedding.gateway.EmbeddingGateway;
import java.util.Optional;

/**
 * The type of model that was used, such as embedding or reranking.
 *
 * <p>Keeps in parity with the grpc proto definition in embedding_gateway.proto
 */
public enum ModelType {
  /** The input type is not specified, for parity with grpc */
  MODEL_TYPE_UNSPECIFIED,
  EMBEDDING,
  RERANKING;

  public static Optional<ModelType> fromEmbeddingGateway(
      EmbeddingGateway.ModelUsage.ModelType modelType) {
    return switch (modelType) {
      case MODEL_TYPE_UNSPECIFIED -> Optional.of(MODEL_TYPE_UNSPECIFIED);
      case EMBEDDING -> Optional.of(EMBEDDING);
      case RERANKING -> Optional.of(RERANKING);
      default -> Optional.empty();
    };
  }

  /** Returns the name used in billing event_type, e.g. {@code "embeddings"} or {@code "reranking"}. */
  public String billingName() {
    return switch (this) {
      case EMBEDDING -> "embeddings";
      case RERANKING -> "reranking";
      case MODEL_TYPE_UNSPECIFIED -> "unspecified";
    };
  }

  public EmbeddingGateway.ModelUsage.ModelType toEmbeddingGateway() {
    return switch (this) {
      case MODEL_TYPE_UNSPECIFIED -> EmbeddingGateway.ModelUsage.ModelType.MODEL_TYPE_UNSPECIFIED;
      case EMBEDDING -> EmbeddingGateway.ModelUsage.ModelType.EMBEDDING;
      case RERANKING -> EmbeddingGateway.ModelUsage.ModelType.RERANKING;
    };
  }
}
