package io.stargate.sgv2.jsonapi.service.embedding.operation;

/** Used to track the metric at a request level to the embedding service */
public class VectorizeUsage {
  private int requestBytes = 0;
  private int responseBytes = 0;
  private int totalTokens = 0;
  private String provider = "";
  private String model = "";

  public VectorizeUsage(
      int requestBytes, int responseBytes, int totalTokens, String provider, String model) {
    this.requestBytes = requestBytes;
    this.responseBytes = responseBytes;
    this.totalTokens = totalTokens;
    this.provider = provider;
    this.model = model;
  }

  public VectorizeUsage() {
    super();
  }

  public VectorizeUsage(String provider, String model) {
    super();
    this.provider = provider;
    this.model = model;
  }

  public void merge(VectorizeUsage vectorizeUsage) {
    this.requestBytes += vectorizeUsage.getRequestBytes();
    this.responseBytes += vectorizeUsage.getResponseBytes();
    this.totalTokens += vectorizeUsage.getTotalTokens();
    this.provider = vectorizeUsage.getProvider();
    this.model = vectorizeUsage.getModel();
  }

  public int getRequestBytes() {
    return requestBytes;
  }

  public int getResponseBytes() {
    return responseBytes;
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

  public void setRequestBytes(int requestBytes) {
    this.requestBytes = requestBytes;
  }

  public void setResponseBytes(int responseBytes) {
    this.responseBytes = responseBytes;
  }

  public void setTotalTokens(int totalTokens) {
    this.totalTokens = totalTokens;
  }
}
