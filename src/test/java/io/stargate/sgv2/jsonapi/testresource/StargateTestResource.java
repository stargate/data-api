//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package io.stargate.sgv2.jsonapi.testresource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

public abstract class StargateTestResource
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {
  private static final Logger LOG = LoggerFactory.getLogger(StargateTestResource.class);

  // Astra will have 8kB limit some time around or after 1.0.0-BETA-6:
  protected static final int DEFAULT_SAI_MAX_STRING_TERM_SIZE_KB = 8;
  private Map<String, String> initArgs;
  protected Optional<String> containerNetworkId;
  private Network network;
  private GenericContainer<?> cassandraContainer;
  private GenericContainer<?> stargateContainer;

  public StargateTestResource() {}

  public void setIntegrationTestContext(DevServicesContext context) {
    this.containerNetworkId = context.containerNetworkId();
  }

  public void init(Map<String, String> initArgs) {
    this.initArgs = initArgs;
  }

  public Map<String, String> start() {
    if (this.shouldSkip()) {
      return Collections.emptyMap();
    } else {
      boolean reuse = false;
      ImmutableMap.Builder propsBuilder;
      if (this.containerNetworkId.isPresent()) {
        String networkId = this.containerNetworkId.get();
        propsBuilder = this.startWithContainerNetwork(networkId, reuse);
      } else {
        propsBuilder = this.startWithoutContainerNetwork(reuse);
      }

      if (useCoordinator()) {
        Integer authPort = this.stargateContainer.getMappedPort(8081);
        String token = this.getAuthToken(this.stargateContainer.getHost(), authPort);
        LOG.info("Using auth token %s for integration tests.".formatted(token));
        propsBuilder.put("stargate.int-test.auth-token", token);
        String cqlPort = this.stargateContainer.getMappedPort(9042).toString();
        propsBuilder.put("stargate.int-test.coordinator.cql-port", cqlPort);
      }

      propsBuilder.put(
          "stargate.int-test.cassandra.host",
          this.cassandraContainer.getCurrentContainerInfo().getConfig().getHostName());
      propsBuilder.put(
          "stargate.int-test.cassandra.cql-port",
          this.cassandraContainer.getMappedPort(9042).toString());

      propsBuilder.put("stargate.int-test.cluster.persistence", getPersistenceModule());
      // Many ITs create more Collections than default Max 5, use more than 50 indexes so:
      propsBuilder.put(
          "stargate.database.limits.max-collections",
          String.valueOf(getMaxCollectionsPerDBOverride()));
      propsBuilder.put(
          "stargate.database.limits.indexes-available-per-database",
          String.valueOf(getIndexesPerDBOverride()));
      propsBuilder.put(
          "stargate.jsonapi.operations.max-count-limit", String.valueOf(getMaxCountLimit()));
      propsBuilder.put(
          "stargate.jsonapi.operations.default-count-page-size",
          String.valueOf(getCountPageSize()));
      Long maxToSort = getMaxDocumentSortCount();
      if (maxToSort != null) {
        propsBuilder.put(
            "stargate.jsonapi.operations.max-document-sort-count", String.valueOf(maxToSort));
      }
      propsBuilder.put("stargate.jsonapi.operations.vectorize-enabled", "true");

      ImmutableMap<String, String> props = propsBuilder.build();
      props.forEach(System::setProperty);
      LOG.info("Using props map for the integration tests: %s".formatted(props));
      return props;
    }
  }

  private boolean shouldSkip() {
    return System.getProperty("quarkus.http.test-host") != null;
  }

  public ImmutableMap.Builder<String, String> startWithoutContainerNetwork(boolean reuse) {
    Network network = this.network();
    this.cassandraContainer = this.baseCassandraContainer(reuse);
    this.cassandraContainer.withNetwork(network);
    this.cassandraContainer.start();
    if (useCoordinator()) {
      this.stargateContainer = this.baseCoordinatorContainer(reuse);
      this.stargateContainer.withNetwork(network).withEnv("SEED", "cassandra");
      this.stargateContainer.start();
      Integer bridgePort = this.stargateContainer.getMappedPort(8091);
      ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();
      propsBuilder.put("quarkus.grpc.clients.bridge.port", String.valueOf(bridgePort));
      return propsBuilder;
    } else {
      ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();
      return propsBuilder;
    }
  }

  private ImmutableMap.Builder<String, String> startWithContainerNetwork(
      String networkId, boolean reuse) {
    this.cassandraContainer = this.baseCassandraContainer(reuse);
    this.cassandraContainer.withNetworkMode(networkId);
    this.cassandraContainer.start();
    String cassandraHost =
        this.cassandraContainer.getCurrentContainerInfo().getConfig().getHostName();
    if (useCoordinator()) {
      this.stargateContainer = this.baseCoordinatorContainer(reuse);
      this.stargateContainer
          .withNetworkMode(networkId)
          .withEnv("BIND_TO_LISTEN_ADDRESS", "true")
          .withEnv("SEED", cassandraHost);
      this.stargateContainer.start();
      String stargateHost =
          this.stargateContainer.getCurrentContainerInfo().getConfig().getHostName();
      ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();
      propsBuilder.put("quarkus.grpc.clients.bridge.host", stargateHost);
      return propsBuilder;
    } else {
      ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();
      return propsBuilder;
    }
  }

  public void stop() {
    if (null != this.cassandraContainer && !this.cassandraContainer.isShouldBeReused()) {
      this.cassandraContainer.stop();
    }

    if (null != this.stargateContainer && !this.stargateContainer.isShouldBeReused()) {
      this.stargateContainer.stop();
    }
  }

  private GenericContainer<?> baseCassandraContainer(boolean reuse) {
    String image = this.getCassandraImage();
    GenericContainer<?> container;
    if (this.isDse()) {
      container =
          new GenericContainer<>(image)
              .withCopyFileToContainer(
                  MountableFile.forClasspathResource("dse.yaml"),
                  "/opt/dse/resources/dse/conf/dse.yaml");
    } else if (this.isHcd()) {
      container =
          new GenericContainer<>(image)
              .withCopyFileToContainer(
                  MountableFile.forClasspathResource("cassandra-hcd.yaml"),
                  "/opt/hcd/resources/cassandra/conf/cassandra.yaml");
    } else {
      container =
          new GenericContainer<>(image)
              .withCopyFileToContainer(
                  MountableFile.forClasspathResource("cassandra.yaml"),
                  "/etc/cassandra/cassandra.yaml");
    }
    final String JVM_EXTRA_OPTS =
        "-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.load_ring_state=false -Dcassandra.initial_token=1 -Dcassandra.sai.max_string_term_size_kb=8"
            // 18-Mar-2025, tatu: to work around [https://github.com/riptano/cndb/issues/13330],
            // need to temporarily add this for HCD:
            + " -Dcassandra.cluster_version_provider.min_stable_duration_ms=-1"
            // 02-May-2025, tatu: [data-api#2063] force checking of max analyzed text length
            + " -Dcassandra.sai.validate_max_term_size_at_coordinator=true";
    container
        .withEnv("HEAP_NEWSIZE", "512M")
        .withEnv("MAX_HEAP_SIZE", "2048M")
        .withEnv("CASSANDRA_CGROUP_MEMORY_LIMIT", "true")
        .withEnv("JVM_EXTRA_OPTS", JVM_EXTRA_OPTS)
        .withNetworkAliases(new String[] {"cassandra"})
        .withExposedPorts(new Integer[] {7000, 9042})
        .withLogConsumer(
            (new Slf4jLogConsumer(LoggerFactory.getLogger("cassandra-docker")))
                .withPrefix("CASSANDRA"));
    if (isHcd() || isDse()) {
      container
          .waitingFor(
              Wait.forSuccessfulCommand(
                  "cqlsh -u cassandra -p cassandra -e \"describe keyspaces\""))
          .withEnv("CLUSTER_NAME", getClusterName())
          .withEnv("DS_LICENSE", "accept");
    } else {
      container
          .waitingFor(Wait.forLogMessage(".*Created default superuser role.*\\n", 1))
          .withEnv("CASSANDRA_CLUSTER_NAME", getClusterName());
    }
    container.withStartupTimeout(this.getCassandraStartupTimeout()).withReuse(reuse);
    return container;
  }

  private GenericContainer<?> baseCoordinatorContainer(boolean reuse) {
    String image = this.getStargateImage();

    String javaOpts =
        String.format(
            "-Xmx1G -D%s=%d",
            "cassandra.sai.max_string_term_size_kb", DEFAULT_SAI_MAX_STRING_TERM_SIZE_KB);

    GenericContainer<?> container =
        (new GenericContainer(image))
            .withEnv("JAVA_OPTS", javaOpts)
            .withEnv("CLUSTER_NAME", getClusterName())
            .withEnv("SIMPLE_SNITCH", "true")
            .withEnv("ENABLE_AUTH", "true")
            .withNetworkAliases(new String[] {"coordinator"})
            .withExposedPorts(new Integer[] {8091, 8081, 8084, 9042})
            .withLogConsumer(
                (new Slf4jLogConsumer(LoggerFactory.getLogger("coordinator-docker")))
                    .withPrefix("COORDINATOR"))
            .waitingFor(Wait.forHttp("/checker/readiness").forPort(8084).forStatusCode(200))
            .withStartupTimeout(this.getCoordinatorStartupTimeout())
            .withReuse(reuse);
    if (this.isDse()) {
      container.withEnv("DSE", "1");
    }

    return container;
  }

  private Network network() {
    if (null == this.network) {
      this.network = Network.newNetwork();
    }

    return this.network;
  }

  private String getCassandraImage() {
    String image = System.getProperty("testing.containers.cassandra-image");
    return null == image ? "cassandra:4.0.10" : image;
  }

  private String getStargateImage() {
    String image = System.getProperty("testing.containers.stargate-image");
    return null == image ? "stargateio/coordinator-4_0:v2.1" : image;
  }

  private static String getClusterName() {
    return System.getProperty("testing.containers.cluster-name", "int-test-cluster");
  }

  public static String getPersistenceModule() {
    return System.getProperty(
        "testing.containers.cluster-persistence", "persistence-cassandra-4.0");
  }

  public static boolean isDse() {
    String dse = System.getProperty("testing.containers.cluster-dse", null);
    return "true".equals(dse);
  }

  public static boolean isHcd() {
    String dse = System.getProperty("testing.containers.cluster-hcd", null);
    return "true".equals(dse);
  }

  public static boolean isRunningUnderMaven() {
    // Running under Maven if surefire test class path is set
    // (note: also set up by Failsafe plugin (integ tests))
    return System.getProperty("surefire.test.class.path") != null;
  }

  /**
   * Returns if coordinator should be started and used.
   *
   * @return
   */
  protected boolean useCoordinator() {
    return Boolean.getBoolean("testing.containers.use-coordinator");
  }

  private Duration getCassandraStartupTimeout() {
    long cassandraStartupTimeout = Long.getLong("testing.containers.cassandra-startup-timeout", 5L);
    return Duration.ofMinutes(cassandraStartupTimeout);
  }

  private Duration getCoordinatorStartupTimeout() {
    long coordinatorStartupTimeout =
        Long.getLong("testing.containers.coordinator-startup-timeout", 3L);
    return Duration.ofMinutes(coordinatorStartupTimeout);
  }

  private String getAuthToken(String host, int authPort) {
    try {
      String json = "{\n  \"username\":\"cassandra\",\n  \"password\":\"cassandra\"\n}\n";
      URI authUri = new URI("http://%s:%d/v1/auth".formatted(host, authPort));
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(authUri)
              .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
              .POST(BodyPublishers.ofString(json))
              .build();
      HttpResponse<String> response =
          HttpClient.newHttpClient().send(request, BodyHandlers.ofString());
      ObjectMapper objectMapper = new ObjectMapper();
      AuthResponse authResponse = objectMapper.readValue(response.body(), AuthResponse.class);
      return authResponse.authToken;
    } catch (Exception var9) {
      throw new RuntimeException("Failed to get Cassandra token for integration tests.", var9);
    }
  }

  public abstract int getMaxCollectionsPerDBOverride();

  public abstract int getIndexesPerDBOverride();

  public abstract int getMaxCountLimit();

  public abstract int getCountPageSize();

  public abstract Long getMaxDocumentSortCount();

  record AuthResponse(String authToken) {}
}
