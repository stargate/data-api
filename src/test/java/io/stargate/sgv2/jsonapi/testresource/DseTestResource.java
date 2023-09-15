package io.stargate.sgv2.jsonapi.testresource;

import io.stargate.sgv2.common.testresource.StargateTestResource;
import java.util.Map;

public class DseTestResource extends StargateTestResource {

  // set default props if not set, so we launch DSE
  // this is only needed for test from the IDE
  public DseTestResource() {
    super();

    if (null == System.getProperty("testing.containers.cassandra-image")) {
      System.setProperty(
          "testing.containers.cassandra-image", "stargateio/dse-next:4.0.7-336cdd7405ee");
    }

    if (null == System.getProperty("testing.containers.stargate-image")) {
      System.setProperty(
          "testing.containers.stargate-image", "stargateio/coordinator-dse-next:v2.1");
    }

    if (null == System.getProperty("testing.containers.cluster-persistence")) {
      System.setProperty("testing.containers.cluster-persistence", "persistence-dse-next");
    }

    if (null == System.getProperty("testing.containers.cluster-dse")) {
      System.setProperty("testing.containers.cluster-dse", "false");
    }
  }

  @Override
  public Map<String, String> start() {
    Map<String, String> env = super.start();
    if (null != System.getProperty("stargate.jsonapi.embedding.service.custom.enabled")) {
      env.put(
          "stargate.jsonapi.embedding.service.custom.enabled",
          System.getProperty("stargate.jsonapi.embedding.service.custom.enabled"));
      env.put(
          "stargate.jsonapi.embedding.service.custom.class-name",
          System.getProperty("stargate.jsonapi.embedding.service.custom.class-name"));
    }
    return env;
  }
}
