package io.stargate.sgv2.jsonapi.service.provider;

import io.stargate.embedding.gateway.EmbeddingGateway;
import java.util.Optional;

/**
 * If the model usage was for indexing data or searching data
 *
 * <p>Keeps in parity with the grpc proto definition in embedding_gateway.proto
 */
public enum ModelType {
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
}
