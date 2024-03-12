package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class PropertyBasedOverrideProfile implements QuarkusTestProfile {
  @Override
  public boolean disableGlobalTestResources() {
    return true;
  }

  @Override
  public Map<String, String> getConfigOverrides() {
    return ImmutableMap.<String, String>builder()
        .put("jsonapi.embedding.providers.openai.enabled", "true")
        .put("jsonapi.embedding.providers.openai.api-key", "openai-api-key")
        .put("jsonapi.embedding.providers.openai.url", "https://api.openai.com/v1/")
        .put("jsonapi.embedding.providers.huggingface.enabled", "true")
        .put("jsonapi.embedding.providers.huggingface.api-key", "hf-api-key")
        .put("jsonapi.embedding.providers.huggingface.url", "https://api-inference.huggingface.co")
        .put("jsonapi.embedding.providers.vertexai.enabled", "false")
        .build();
  }
}
