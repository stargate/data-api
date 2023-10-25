package io.stargate.sgv2.jsonapi.testresource;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Collections;
import java.util.Map;

public class JsonApiTestProfile implements QuarkusTestProfile {

  @Override
  public Map<String, String> getConfigOverrides() {
    return Collections.singletonMap("sda.debug.dsa", "true");
  }
}
