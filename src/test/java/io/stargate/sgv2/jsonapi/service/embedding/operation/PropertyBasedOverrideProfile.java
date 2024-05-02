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
        .put("stargate.jsonapi.embedding.providers.openai.enabled", "true")
        .put("stargate.jsonapi.embedding.providers.openai.url", "https://api.openai.com/v1/")
        .put("stargate.jsonapi.embedding.providers.huggingface.enabled", "true")
        .put(
            "stargate.jsonapi.embedding.providers.huggingface.url",
            "https://api-inference.huggingface.co")
        .put("stargate.jsonapi.embedding.providers.vertexai.enabled", "false")
        .build();
  }
}
