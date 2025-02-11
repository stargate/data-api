package io.stargate.sgv2.jsonapi.service.embedding.operation;

/** Used to track the metric at a request level to the embedding service */
public class VectorizeUsageInfo {
  private int requestSize;
  private int responseSize;
  private int totalTokens;
  private String provider;
  private String model;

  public VectorizeUsageInfo(
      int requestSize, int responseSize, int totalTokens, String provider, String model) {
    this.requestSize = requestSize;
    this.responseSize = responseSize;
    this.totalTokens = totalTokens;
    this.provider = provider;
    this.model = model;
  }

  public int getRequestSize() {
    return requestSize;
  }

  public int getResponseSize() {
    return responseSize;
  }

  public int getTotalTokens() {
    return totalTokens;
  }

  public String getProvider() {
    return provider;
  }

  public String getModel() {
    return model;
  }
}
