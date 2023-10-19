package io.stargate.sgv2.jsonapi.api.v1;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy;
import io.stargate.sgv2.common.IntegrationTestUtils;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

/**
 * Base test class that exposes a Java driver {@link CqlSession} connected to the Stargate
 * coordinator, allowing subclasses to create and populate their CQL schema.
 *
 * <p>Subclasses must be annotated with {@link io.quarkus.test.junit.QuarkusIntegrationTest}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class CqlEnabledIntegrationTestBase {

  protected final CqlIdentifier keyspaceId =
      CqlIdentifier.fromInternal(
          System.getProperty("stargate.jsonapi.operations.keyspace", "default_keyspace"));

  protected CqlSession session;

  @BeforeAll
  public final void buildSession() {
    OptionsMap config = OptionsMap.driverDefaults();
    config.put(TypedDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10));
    config.put(TypedDriverOption.REQUEST_WARN_IF_SET_KEYSPACE, false);
    config.put(
        TypedDriverOption.LOAD_BALANCING_POLICY_CLASS,
        DcInferringLoadBalancingPolicy.class.getName());

    // resolve auth if enabled
    if (IntegrationTestUtils.isCassandraAuthEnabled()) {
      config.put(TypedDriverOption.AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class.getName());
      config.put(
          TypedDriverOption.AUTH_PROVIDER_USER_NAME, IntegrationTestUtils.getCassandraUsername());
      config.put(
          TypedDriverOption.AUTH_PROVIDER_PASSWORD, IntegrationTestUtils.getCassandraPassword());
    }

    session =
        CqlSession.builder()
            .addContactPoint(IntegrationTestUtils.getCassandraCqlAddress())
            .withConfigLoader(DriverConfigLoader.fromMap(config))
            .build();
  }

  @BeforeAll
  public final void createKeyspace() {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
            .formatted(keyspaceId.asCql(false)));
    session.execute("USE %s".formatted(keyspaceId.asCql(false)));
  }

  @AfterAll
  public final void cleanUp() {
    if (session != null) {
      session.execute("DROP KEYSPACE IF EXISTS %s".formatted(keyspaceId.asCql(false)));
      session.close();
    }
  }
}
