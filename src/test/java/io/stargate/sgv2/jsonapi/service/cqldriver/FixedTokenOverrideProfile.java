package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class FixedTokenOverrideProfile implements QuarkusTestProfile {

  @Override
  public Map<String, String> getConfigOverrides() {
    return ImmutableMap.<String, String>builder()
        .put("stargate.jsonapi.operations.database-config.fixed-token", "test-token")
        .build();
  }
}
