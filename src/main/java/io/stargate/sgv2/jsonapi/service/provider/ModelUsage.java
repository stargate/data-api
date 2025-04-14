package io.stargate.sgv2.jsonapi.service.provider;

import io.stargate.embedding.gateway.EmbeddingGateway;

/**
 * This class is to track the usage at the http request level to the embedding or reranking provider
 * model service.
 */
public class ModelUsage {

  public final ProviderType providerType;
  public final String provider;
  public final String model;

  private int requestBytes = 0;
  private int responseBytes = 0;

  private int promptTokens = 0;
  private int totalTokens = 0;

  public ModelUsage(ProviderType providerType, String provider, String model) {
    this.providerType = providerType;
    this.provider = provider;
    this.model = model;
  }

  public ModelUsage(
      ProviderType providerType,
      String provider,
      String model,
      int requestBytes,
      int responseBytes,
      int promptTokens,
      int totalTokens) {
    this.providerType = providerType;
    this.provider = provider;
    this.model = model;
    this.requestBytes = requestBytes;
    this.responseBytes = responseBytes;
    this.promptTokens = promptTokens;
    this.totalTokens = totalTokens;
  }

  /** Create the ModelUsage from the modelUsage of Embedding Gateway gRPC response. */
  public static ModelUsage fromGrpcResponse(EmbeddingGateway.ModelUsage modelUsage) {
    return new ModelUsage(
        ProviderType.valueOf(modelUsage.getProviderType()),
        modelUsage.getProviderName(),
        modelUsage.getModelName(),
        modelUsage.getRequestBytes(),
        modelUsage.getResponseBytes(),
        modelUsage.getPromptTokens(),
        modelUsage.getTotalTokens());
  }

  /**
   * Parse the request and response bytes from the headers of the intercepted response. Headers are
   * added in the {@link ProviderHttpInterceptor} registered by specified providerClient.
   */
  public ModelUsage parseSentReceivedBytes(jakarta.ws.rs.core.Response interceptedResp) {
    if (interceptedResp.getHeaders().get(ProviderHttpInterceptor.SENT_BYTES_HEADER) != null) {
      this.requestBytes =
          Integer.parseInt(
              interceptedResp.getHeaderString(ProviderHttpInterceptor.SENT_BYTES_HEADER));
    }
    if (interceptedResp.getHeaders().get(ProviderHttpInterceptor.RECEIVED_BYTES_HEADER) != null) {
      this.responseBytes =
          Integer.parseInt(
              interceptedResp.getHeaderString(ProviderHttpInterceptor.RECEIVED_BYTES_HEADER));
    }
    return this;
  }

  public ModelUsage setPromptTokens(int promptTokens) {
    this.promptTokens = promptTokens;
    return this;
  }

  public ModelUsage setTotalTokens(int totalTokens) {
    this.totalTokens = totalTokens;
    return this;
  }

  public int getRequestBytes() {
    return requestBytes;
  }

  public int getResponseBytes() {
    return responseBytes;
  }

  public int getPromptTokens() {
    return promptTokens;
  }

  public int getTotalTokens() {
    return totalTokens;
  }

  @Override
  public String toString() {
    return "ModelUsage{"
        + "providerType="
        + providerType
        + ", provider='"
        + provider
        + '\''
        + ", model='"
        + model
        + '\''
        + ", requestBytes="
        + requestBytes
        + ", responseBytes="
        + responseBytes
        + ", promptTokens="
        + promptTokens
        + ", totalTokens="
        + totalTokens
        + '}';
  }
}
