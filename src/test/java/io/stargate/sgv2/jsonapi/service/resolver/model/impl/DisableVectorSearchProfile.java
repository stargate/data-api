package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class DisableVectorSearchProfile implements QuarkusTestProfile {
  @Override
  public boolean disableGlobalTestResources() {
    return true;
  }

  @Override
  public Map<String, String> getConfigOverrides() {
    return ImmutableMap.<String, String>builder()
        .put("stargate.data-store.vector-search-enabled", "false")
        .build();
  }
}
