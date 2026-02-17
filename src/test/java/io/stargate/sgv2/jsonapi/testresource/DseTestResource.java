package io.stargate.sgv2.jsonapi.testresource;

import static io.stargate.sgv2.jsonapi.api.v1.util.IntegrationTestUtils.CASSANDRA_CQL_HOST_PROP;
import static io.stargate.sgv2.jsonapi.api.v1.util.IntegrationTestUtils.CASSANDRA_CQL_PORT_PROP;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test resource for Cassandra-via-Docket -backed Integration Tests. Note that "Dse" in name is not
 * strictly accurate, but kept for backwards compatibility: may run HCD as the backend for example.
 */
public class DseTestResource extends StargateTestResource {
  private static final Logger LOG = LoggerFactory.getLogger(DseTestResource.class);

  // Need some additional pre-configuration when NOT running under Maven
  public DseTestResource() {
    if (isRunningUnderMaven()) {
      LOG.info("Running under Maven, no need to overwrite integration test properties");
      return;
    }

    LOG.info("NOT Running under Maven, will overwrite integration test properties");

    final String cassandraImage = loadCassandraImageFromDockerComposeEnv();
    LOG.info("Cassandra image to use for Integration Tests: " + cassandraImage);

    System.setProperty("testing.containers.cassandra-image", cassandraImage);

    // MUST set one of these to get DS_LICENSE env var set
    // System.setProperty("testing.containers.cluster-dse", "true");
    System.setProperty("testing.containers.cluster-hcd", "true");

    // 14-Mar-2025, tatu: We no longer run Stargate Coordinator for ITs set up removed
  }

  private String loadCassandraImageFromDockerComposeEnv() {
    // 21-Apr-2025, tatu: formerly referenced hard-coded images; left here for reference:
    //   to be removed in near future
    // "stargateio/dse-next:4.0.11-591d171ac9c9"
    // "datastax/dse-server:6.9.18"
    // "559669398656.dkr.ecr.us-west-2.amazonaws.com/engops-shared/hcd/prod/hcd:1.2.3";

    // 21-Apr-2025, tatu: [data-api#1952] Load definition from "./docker-compose/.env"
    final File inputFile = new File("docker-compose/.env").getAbsoluteFile();
    LOG.info("Loading Cassandra image definition from: " + inputFile);
    Properties props = new Properties();
    try (FileInputStream fis = new FileInputStream(inputFile)) {
      props.load(fis);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to load Properties from file: " + inputFile, e);
    }

    String image = nonEmptyProp(inputFile, props, "HCDIMAGE");
    String tag = nonEmptyProp(inputFile, props, "HCDTAG");

    return image + ":" + tag;
  }

  private String nonEmptyProp(File inputFile, Properties props, String key) {
    String value = props.getProperty(key);
    if (value == null || value.isEmpty()) {
      throw new IllegalStateException(
          "Properties from file: '" + inputFile + "' are missing required property: " + key);
    }
    if (value.startsWith("\"") && value.endsWith("\"")) {
      value = value.substring(1, value.length() - 1);
    }
    return value;
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

  // Set to 100 from 10,000 for easier testing
  public Long getMaxDocumentSortCount() {
    return 100L;
  }

  // By default, allow Lexical on HCD backend, but not on DSE
  public String getFeatureFlagLexical() {
    return isHcd() ? "true" : "false";
  }

  // By default, we enable the feature flag for reranking
  public String getFeatureFlagReranking() {
    return "true";
  }

  @Override
  public Map<String, String> start() {
    Map<String, String> env = super.start();
    ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();
    propsBuilder.putAll(env);

    // 02-April-2025, yuqi: [data-api#1972] Set the system property variable to override the
    // provider config file resource.
    // Note, this only helps local integration runs, not GitHub integration test actions.
    // For GitHub actions, the system property is passing through script in CI workflow file.
    propsBuilder.put("RERANKING_CONFIG_RESOURCE", "test-reranking-providers-config.yaml");
    propsBuilder.put("EMBEDDING_CONFIG_RESOURCE", "test-embedding-providers-config.yaml");

    propsBuilder.put("stargate.jsonapi.custom.embedding.enabled", "true");

    // 17-Mar-2025, tatu: [data-api#1903] Lexical search/sort feature flag
    String lexicalFeatureSetting = getFeatureFlagLexical();
    if (lexicalFeatureSetting != null) {
      propsBuilder.put("stargate.feature.flags.lexical", lexicalFeatureSetting);
    }

    // 31-Mar-2025, yuqi: [data-api#1904] Reranking feature flag:
    String featureFlagReranking = getFeatureFlagReranking();
    if (featureFlagReranking != null) {
      propsBuilder.put("stargate.feature.flags.reranking", featureFlagReranking);
    }

    propsBuilder.put(
        "stargate.jsonapi.custom.embedding.clazz",
        "io.stargate.sgv2.jsonapi.service.embedding.operation.test.CustomITEmbeddingProvider");
    propsBuilder.put(
        "stargate.jsonapi.embedding.providers.huggingface.supported-authentications.HEADER.enabled",
        "false");
    propsBuilder.put(
        "stargate.jsonapi.embedding.providers.openai.supported-authentications.SHARED_SECRET.enabled",
        "true");
    propsBuilder.put("stargate.jsonapi.embedding.providers.vertexai.enabled", "true");
    propsBuilder.put(
        "stargate.jsonapi.embedding.providers.vertexai.models[0].parameters[0].required", "true");

    // Prefer instance-specific configuration from 'env' to support parallel execution and
    // isolation. Fall back to global system properties only if instance-specific values are
    // missing.
    if (this.containerNetworkId.isPresent()) {
      String host =
          env.getOrDefault(CASSANDRA_CQL_HOST_PROP, System.getProperty(CASSANDRA_CQL_HOST_PROP));
      propsBuilder.put("stargate.jsonapi.operations.database-config.cassandra-end-points", host);
    } else {
      String port =
          env.getOrDefault(CASSANDRA_CQL_PORT_PROP, System.getProperty(CASSANDRA_CQL_PORT_PROP));
      propsBuilder.put("stargate.jsonapi.operations.database-config.cassandra-port", port);
    }

    if (isDse() || isHcd()) {
      propsBuilder.put("stargate.jsonapi.operations.database-config.local-datacenter", "dc1");
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
