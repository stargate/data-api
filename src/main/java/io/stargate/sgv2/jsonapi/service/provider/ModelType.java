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

  /**
   * Name of the model type used in the billing event name.
   *
   * @throws IllegalArgumentException for {@link #MODEL_TYPE_UNSPECIFIED}, it cannot be billed
   */
  public String billingEventName() {
    return switch (this) {
      case MODEL_TYPE_UNSPECIFIED ->
          throw new IllegalArgumentException(
              "ModelType.billingEventName() - MODEL_TYPE_UNSPECIFIED has no billing event name");
      case EMBEDDING -> "embedding";
      case RERANKING -> "reranking";
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
