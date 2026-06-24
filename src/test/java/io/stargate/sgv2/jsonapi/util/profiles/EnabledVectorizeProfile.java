package io.stargate.sgv2.jsonapi.util.profiles;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

/**
 * Enables vectorize via operations config, while also disables spinning up DB as a test resource.
 */
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
