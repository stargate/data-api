package io.stargate.sgv2.jsonapi.api.model.command;

import jakarta.enterprise.context.RequestScoped;

/**
 * Bean class which is used to return as response json in header
 */
@RequestScoped
public class VectorizeUsageBean {
  private int requestSize;
  private int responseSize;
  private int totalTokens;
  private String provider;
  private String model;

  public int getRequestSize() {
    return requestSize;
  }

  public void setRequestSize(int requestSize) {
    this.requestSize = requestSize;
  }

  public int getResponseSize() {
    return responseSize;
  }

  public void setResponseSize(int responseSize) {
    this.responseSize = responseSize;
  }

  public int getTotalTokens() {
    return totalTokens;
  }

  public void setTotalTokens(int totalTokens) {
    this.totalTokens = totalTokens;
  }

  public String getProvider() {
    return provider;
  }

  public void setProvider(String provider) {
    this.provider = provider;
  }

  public String getModel() {
    return model;
  }

  public void setModel(String model) {
    this.model = model;
  }
}
