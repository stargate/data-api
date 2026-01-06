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
  public void stop() {
    super.stop();
    isolatedContainer = null;
  }

  public static GenericContainer<?> getIsolatedContainer() {
    return isolatedContainer;
  }
}
