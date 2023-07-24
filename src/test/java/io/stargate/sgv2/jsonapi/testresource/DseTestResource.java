package io.stargate.sgv2.jsonapi.testresource;

import io.stargate.sgv2.common.testresource.StargateTestResource;

public class DseTestResource extends StargateTestResource {

  // set default props if not set, so we launch DSE
  // this is only needed for test from the IDE
  public DseTestResource() {
    super();

    if (null == System.getProperty("testing.containers.cassandra-image")) {
      System.setProperty("testing.containers.cassandra-image", "datastax/dse-next:4.0.7-0acaae364c19");
    }

    if (null == System.getProperty("testing.containers.stargate-image")) {
      System.setProperty("testing.containers.stargate-image", "stargateio/coordinator-dse-next:v2.1");
    }

    if (null == System.getProperty("testing.containers.cluster-persistence")) {
      System.setProperty("testing.containers.cluster-persistence", "persistence-dse-next");
    }

    if (null == System.getProperty("testing.containers.cluster-dse")) {
      System.setProperty("testing.containers.cluster-dse", "false");
    }
  }
}
