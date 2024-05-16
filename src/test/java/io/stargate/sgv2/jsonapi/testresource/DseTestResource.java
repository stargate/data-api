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
          "testing.containers.cassandra-image", "stargateio/dse-next:4.0.11-591d171ac9c9");
    }

    if (null == System.getProperty("testing.containers.stargate-image")) {
      // 07-Dec-2023, tatu: For some reason floating tag "v2.1" does not seem to work so
      //    use specific version. Needs to be kept up to date:
      System.setProperty(
          "testing.containers.stargate-image", "stargateio/coordinator-dse-next:v2.1.0-BETA-8");
    }

    if (null == System.getProperty("testing.containers.cluster-persistence")) {
      System.setProperty("testing.containers.cluster-persistence", "persistence-dse-next");
    }

    if (null == System.getProperty("testing.containers.cluster-dse")) {
      System.setProperty("testing.containers.cluster-dse", "false");
    }

    if (null == System.getProperty("cassandra.sai.max_string_term_size_kb")) {
      System.setProperty(
          "cassandra.sai.max_string_term_size_kb",
          String.valueOf(DEFAULT_SAI_MAX_STRING_TERM_SIZE_KB));
    }
  }

  // Many tests create more than 5 collections so default to 10
  @Override
  public int getMaxCollectionsPerDBOverride() {
    return 10;
  }

  // As per requiring up to 10 collections, will also then need 100 SAIs
  @Override
  public int getIndexesPerDBOverride() {
    return 100;
  }

  // Test count with count limit as 5 so more data can be tested
  @Override
  public int getMaxCountLimit() {
    return 5;
  }

  // Setting this to 2 so data read with Pagination
  public int getCountPageSize() {
    return 2;
  }

  @Override
  public Map<String, String> start() {
    Map<String, String> env = super.start();
    ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();
    propsBuilder.putAll(env);
    propsBuilder.put("stargate.jsonapi.custom.embedding.enabled", "true");
    propsBuilder.put(
        "stargate.jsonapi.custom.embedding.clazz",
        "io.stargate.sgv2.jsonapi.service.embedding.operation.test.CustomITEmbeddingProvider");
    propsBuilder.put(
        "stargate.jsonapi.embedding.providers.huggingface.supported-authentications.HEADER.enabled",
        "false");
    propsBuilder.put("stargate.jsonapi.embedding.providers.vertexai.enabled", "true");
    propsBuilder.put(
        "stargate.jsonapi.embedding.providers.vertexai.models[0].parameters[0].required", "true");
    if (this.containerNetworkId.isPresent()) {
      String host =
          useCoordinator()
              ? System.getProperty("quarkus.grpc.clients.bridge.host")
              : System.getProperty("stargate.int-test.cassandra.host");
      propsBuilder.put("stargate.jsonapi.operations.database-config.cassandra-end-points", host);
    } else {
      int port =
          useCoordinator()
              ? Integer.getInteger(IntegrationTestUtils.STARGATE_CQL_PORT_PROP)
              : Integer.getInteger(IntegrationTestUtils.CASSANDRA_CQL_PORT_PROP);
      propsBuilder.put(
          "stargate.jsonapi.operations.database-config.cassandra-port", String.valueOf(port));
    }
    if (useCoordinator()) {
      String defaultToken = System.getProperty(IntegrationTestUtils.AUTH_TOKEN_PROP);
      if (defaultToken != null) {
        propsBuilder.put("stargate.jsonapi.operations.database-config.fixed-token", defaultToken);
      }
    }
    propsBuilder.put("stargate.data-store.ignore-bridge", "true");
    propsBuilder.put("stargate.debug.enabled", "true");
    // Reduce the delay for ITs
    if (Boolean.getBoolean("run-create-index-parallel")) {
      propsBuilder.put("stargate.jsonapi.operations.database-config.ddl-delay-millis", "0");
    } else {
      propsBuilder.put("stargate.jsonapi.operations.database-config.ddl-delay-millis", "50");
    }
    return propsBuilder.build();
  }
}
