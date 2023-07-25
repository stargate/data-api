package io.stargate.sgv2.jsonapi.testresource;

import io.stargate.sgv2.common.testresource.StargateTestResource;

public class DseTestResource extends StargateTestResource {

  // set default props if not set, so we launch DSE
  // this is only needed for test from the IDE
  public DseTestResource() {
    super();

    if (null == System.getProperty("testing.containers.cassandra-image")) {
      System.setProperty("testing.containers.cassandra-image", "datastax/dse-server:6.8.36");
    }

    if (null == System.getProperty("testing.containers.stargate-image")) {
      System.setProperty("testing.containers.stargate-image", "stargateio/coordinator-dse-68:v2");
    }
  }
}
