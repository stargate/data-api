package io.stargate.sgv2.jsonapi.testresource;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class FixedTokenTestResource extends DseTestResource {
  public FixedTokenTestResource() {
    super();
  }

  @Override
  public Map<String, String> start() {
    Map<String, String> env = super.start();
    ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();
    propsBuilder.putAll(env);

    // set fix-token, used by CqlSessionTest
    propsBuilder.put("stargate.jsonapi.operations.database-config.fixed-token", "test-token");
    return propsBuilder.build();
  }
}
