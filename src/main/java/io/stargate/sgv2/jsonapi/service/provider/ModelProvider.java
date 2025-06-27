package io.stargate.sgv2.jsonapi.service.provider;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Identifier for a Model Provider.
 *
 * <p>The list here needs to sync with the list used in the yaml config files, this is the
 * canonnical list of Model Proviers that we know about. aaron 17 june 2025 - This is used to be a
 * series of string consts in a class called ProviderConstants
 */
public enum ModelProvider {
  AZURE_OPENAI("azureOpenAI"),
  BEDROCK("bedrock"),
  COHERE("cohere"),
  CUSTOM("custom"),
  HUGGINGFACE("huggingface"),
  HUGGINGFACE_DEDICATED("huggingfaceDedicated"),
  HUGGINGFACE_DEDICATED_DEFINED_MODEL("endpoint-defined-model"),
  JINA_AI("jinaAI"),
  MISTRAL("mistral"),
  NVIDIA("nvidia"),
  OPENAI("openai"),
  UPSTAGE_AI("upstageAI"),
  VERTEXAI("vertexai"),
  VOYAGE_AI("voyageAI");

  private static final Map<String, ModelProvider> API_NAME_TO_PROVIDER;

  static {
    API_NAME_TO_PROVIDER = new HashMap<>();
    for (ModelProvider provider : ModelProvider.values()) {
      API_NAME_TO_PROVIDER.put(provider.apiName(), provider);
    }
  }

  private final String apiName;

  ModelProvider(String apiName) {
    this.apiName = apiName;
  }

  public String apiName() {
    return apiName;
  }

  public static Optional<ModelProvider> fromApiName(String apiName) {
    return Optional.ofNullable(API_NAME_TO_PROVIDER.get(apiName));
  }

  @Override
  public String toString() {
    return apiName;
  }
}
