package io.stargate.sgv2.jsonapi.testresource;

import java.util.Map;
import org.testcontainers.containers.GenericContainer;

public class IsolatedDseTestResource extends DseTestResource {

  private static GenericContainer<?> isolatedContainer;

  @Override
  public Map<String, String> start() {
    Map<String, String> props = super.start();
    isolatedContainer = super.getCassandraContainer();
    return props;
  }

  @Override
  protected void exposeSystemProperties(Map<String, String> props) {
    // Do not expose system properties to avoid interfering with other tests running in parallel
  }

  @Override
  public void stop() {
    super.stop();
    isolatedContainer = null;
  }

  public static GenericContainer<?> getIsolatedContainer() {
    return isolatedContainer;
  }
}
