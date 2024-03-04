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
        .put("jsonapi.embedding.config.store", "property")
        .put("jsonapi.vector.provider.openai.enabled", "true")
        .put("jsonapi.vector.provider.openai.api-key", "openai-api-key")
        .put("jsonapi.vector.provider.openai.url", "https://api.openai.com/v1/")
        .put("jsonapi.vector.provider.huggingface.enabled", "true")
        .put("jsonapi.vector.provider.huggingface.api-key", "hf-api-key")
        .put("jsonapi.vector.provider.huggingface.url", "https://api-inference.huggingface.co")
        .put("jsonapi.vector.provider.vertexai.enabled", "false")
        .build();
  }
}
