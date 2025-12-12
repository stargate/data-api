package io.stargate.sgv2.jsonapi.testresource;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
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

  protected Optional<String> containerNetworkId;

  /** Shared network for containers to communicate. */
  private Network network;

  /** The backend database container (Cassandra, DSE, or HCD). */
  private GenericContainer<?> cassandraContainer;

  /**
   * Called by Quarkus to inject the DevServicesContext, allowing us to detect if we are running
   * inside a container network.
   */
  @Override
  public void setIntegrationTestContext(DevServicesContext context) {
    this.containerNetworkId = context.containerNetworkId();
  }

  @Override
  public Map<String, String> start() {
    if (this.shouldSkip()) {
      return Collections.emptyMap();
    } else {
      boolean reuse = false;
      ImmutableMap.Builder<String, String> propsBuilder;

      if (this.containerNetworkId.isPresent()) {
        String networkId = this.containerNetworkId.get();
        propsBuilder = this.startWithContainerNetwork(networkId, reuse);
      } else {
        propsBuilder = this.startWithoutContainerNetwork(reuse);
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

  @Override
  public void stop() {
    if (null != this.cassandraContainer && !this.cassandraContainer.isShouldBeReused()) {
      this.cassandraContainer.stop();
    }
  }

  public abstract int getMaxCollectionsPerDBOverride();

  public abstract int getIndexesPerDBOverride();

  public abstract int getMaxCountLimit();

  public abstract int getCountPageSize();

  public abstract Long getMaxDocumentSortCount();

  public static String getPersistenceModule() {
    return System.getProperty(
        "testing.containers.cluster-persistence", "persistence-cassandra-4.0");
  }

  public static boolean isDse() {
    String dse = System.getProperty("testing.containers.cluster-dse", null);
    return "true".equals(dse);
  }

  public static boolean isHcd() {
    String hcd = System.getProperty("testing.containers.cluster-hcd", null);
    return "true".equals(hcd);
  }

  public static boolean isRunningUnderMaven() {
    // Running under Maven if surefire test class path is set
    // (note: also set up by Failsafe plugin (integration tests))
    return System.getProperty("surefire.test.class.path") != null;
  }

  private boolean shouldSkip() {
    return System.getProperty("quarkus.http.test-host") != null;
  }

  private ImmutableMap.Builder<String, String> startWithoutContainerNetwork(boolean reuse) {
    Network network = this.network();
    this.cassandraContainer = this.baseCassandraContainer(reuse);
    this.cassandraContainer.withNetwork(network);
    this.cassandraContainer.start();

    return ImmutableMap.builder();
  }

  private ImmutableMap.Builder<String, String> startWithContainerNetwork(
      String networkId, boolean reuse) {
    this.cassandraContainer = this.baseCassandraContainer(reuse);
    this.cassandraContainer.withNetworkMode(networkId);
    this.cassandraContainer.start();

    return ImmutableMap.builder();
  }

  private GenericContainer<?> baseCassandraContainer(boolean reuse) {
    String image = this.getCassandraImage();
    GenericContainer<?> container;

    // Some JVM options are same for all backends, start with those:
    String JVM_EXTRA_OPTS =
        "-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.load_ring_state=false -Dcassandra.initial_token=1 -Dcassandra.sai.max_string_term_size_kb=8"
            // 18-Mar-2025, tatu: to work around [https://github.com/riptano/cndb/issues/13330],
            // need to temporarily add this for HCD:
            + " -Dcassandra.cluster_version_provider.min_stable_duration_ms=-1"
            // 02-May-2025, tatu: [data-api#2063] force checking of max analyzed text length
            + " -Dcassandra.sai.validate_max_term_size_at_coordinator=true";

    // Important: Start by checking if we are running HCD: default for local testing.
    // (for some reason looks like both "isHcd()" and "isDse()" may return true under
    // some conditions...)
    if (isHcd()) {
      container =
          new GenericContainer<>(image)
              .withCopyFileToContainer(
                  MountableFile.forClasspathResource("cassandra-hcd.yaml"),
                  "/opt/hcd/resources/cassandra/conf/cassandra.yaml");
      // 25-Sep-2025, tatu: HCD 1.2.3 does proper format version checks, need to enable
      // recent enough format:
      JVM_EXTRA_OPTS += " -Dcassandra.sai.latest.version=ec"
      // this MAY be needed too wrt ^^^
      // + " -Dcassandra.sai.jvector_version=4"
      ;
    } else if (isDse()) {
      container =
          new GenericContainer<>(image)
              .withCopyFileToContainer(
                  MountableFile.forClasspathResource("dse.yaml"),
                  "/opt/dse/resources/dse/conf/dse.yaml");
    } else {
      container =
          new GenericContainer<>(image)
              .withCopyFileToContainer(
                  MountableFile.forClasspathResource("cassandra.yaml"),
                  "/etc/cassandra/cassandra.yaml");
    }

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

  private static String getClusterName() {
    return System.getProperty("testing.containers.cluster-name", "int-test-cluster");
  }

  private Duration getCassandraStartupTimeout() {
    long cassandraStartupTimeout = Long.getLong("testing.containers.cassandra-startup-timeout", 5L);
    return Duration.ofMinutes(cassandraStartupTimeout);
  }
}
