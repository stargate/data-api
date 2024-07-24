package io.stargate.sgv2.jsonapi.testresource;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FixedTokenTestResource extends DseTestResource {

  private static final Logger LOG = LoggerFactory.getLogger(FixedTokenTestResource.class);

  public FixedTokenTestResource() {
    super();
  }

  @Override
  public Map<String, String> start() {
    Map<String, String> env = super.start();
    ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();
    propsBuilder.putAll(env);

    // set fix-token, used by CqlSessionTest
    //    propsBuilder.put("stargate.jsonapi.operations.database-config.fixed-token", "test-token");
    ImmutableMap<String, String> props = propsBuilder.build();
    props.forEach(System::setProperty);
    LOG.info(
        "FixedTokenTestResource, Using props map for the integration tests: %s".formatted(props));
    return props;
  }
}
