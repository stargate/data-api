package io.stargate.sgv2.jsonapi.service.resolver;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class EnabledVectorizeProfile implements QuarkusTestProfile {
  @Override
  public boolean disableGlobalTestResources() {
    return true;
  }

  @Override
  public Map<String, String> getConfigOverrides() {
    return ImmutableMap.<String, String>builder()
        .put("stargate.jsonapi.operations.vectorize-enabled", "true")
        .build();
  }
}
