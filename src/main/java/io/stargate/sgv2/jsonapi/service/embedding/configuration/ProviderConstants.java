package io.stargate.sgv2.jsonapi.service.embedding.configuration;

public final class ProviderConstants {
  public static final String OPENAI = "openai";
  public static final String AZURE_OPENAI = "azureOpenAI";
  public static final String HUGGINGFACE = "huggingface";
  public static final String VERTEXAI = "vertexai";
  public static final String COHERE = "cohere";
  public static final String NVIDIA = "nvidia";
  public static final String CUSTOM = "custom";

  // Private constructor to prevent instantiation
  private ProviderConstants() {}
}
