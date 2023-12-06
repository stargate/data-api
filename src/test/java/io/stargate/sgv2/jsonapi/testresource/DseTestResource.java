package io.stargate.sgv2.jsonapi.testresource;

import io.stargate.sgv2.common.IntegrationTestUtils;
import java.util.Map;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class DseTestResource extends StargateTestResource {

  // set default props if not set, so we launch DSE
  // this is only needed for test from the IDE
  public DseTestResource() {
    super();

    if (null == System.getProperty("testing.containers.cassandra-image")) {
      System.setProperty(
          "testing.containers.cassandra-image", "stargateio/dse-next:4.0.11-b259738f492f");
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
    ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();
    propsBuilder.putAll(env);
    propsBuilder.put("stargate.jsonapi.embedding.service.custom.enabled", "true");
    propsBuilder.put(
        "stargate.jsonapi.embedding.service.custom.clazz",
        "io.stargate.sgv2.jsonapi.service.embedding.operation.test.CustomITEmbeddingService");
    if (this.containerNetworkId.isPresent()) {
      String host = System.getProperty("quarkus.grpc.clients.bridge.host");
      propsBuilder.put("stargate.jsonapi.operations.database-config.cassandra-end-points", host);
    } else {
      int port = Integer.getInteger(IntegrationTestUtils.STARGATE_CQL_PORT_PROP);
      propsBuilder.put(
          "stargate.jsonapi.operations.database-config.cassandra-port", String.valueOf(port));
    }

    String defaultToken = System.getProperty(IntegrationTestUtils.AUTH_TOKEN_PROP);
    if (defaultToken != null) {
      propsBuilder.put("stargate.jsonapi.operations.database-config.fixed-token", defaultToken);
    }
    propsBuilder.put("stargate.debug.enabled", "true");
    return propsBuilder.build();
  }
}
