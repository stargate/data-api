package io.stargate.sgv2.jsonapi.testresource;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class SerialConsistencyOverrideProfile implements QuarkusTestProfile {
  @Override
  public boolean disableGlobalTestResources() {
    return true;
  }

  @Override
  public Map<String, String> getConfigOverrides() {
    return ImmutableMap.<String, String>builder()
        .put("stargate.queries.serial-consistency", "LOCAL_SERIAL")
        .build();
  }
}
