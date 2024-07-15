package io.stargate.sgv2.jsonapi.service.embedding.configuration;

public final class ProviderConstants {
  public static final String OPENAI = "openai";
  public static final String AZURE_OPENAI = "azureOpenAI";
  public static final String HUGGINGFACE = "huggingface";
  public static final String HUGGINGFACE_DEDICATED = "huggingfaceDedicated";
  public static final String HUGGINGFACE_DEDICATED_DEFINED_MODEL = "endpoint-defined-model";
  public static final String VERTEXAI = "vertexai";
  public static final String COHERE = "cohere";
  public static final String NVIDIA = "nvidia";
  public static final String UPSTAGE_AI = "upstageAI";
  public static final String VOYAGE_AI = "voyageAI";
  public static final String JINA_AI = "jinaAI";
  public static final String CUSTOM = "custom";
  public static final String MISTRAL = "mistral";
  public static final String BEDROCK = "bedrock";

  // Private constructor to prevent instantiation
  private ProviderConstants() {}
}
