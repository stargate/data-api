package io.stargate.sgv2.jsonapi.service.cqldriver;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

/**
 * Driver config loading tests with Env overrides: separate from basic tests due to static nature of
 * Env Var setup.
 */
@ExtendWith(SystemStubsExtension.class)
public class CqlDriverConfigLoadEnvTests {
  private static final String DEFAULT_SESSION = "tenant-none";

  private static final String TENANT_OVERRIDE = "tenant-ENV";
  private static final String CONSISTENCY_OVERRIDE = "ANY";

  @SystemStub private EnvironmentVariables environmentVariables;

  @BeforeAll
  static void initializeSessionFactory() {
    // Important! CqlSessionFactory sets System property for considering
    // Env var overrides; must be called before tests
    CqlSessionFactory f =
        new CqlSessionFactory(
            "test",
            DatabaseType.CASSANDRA,
            "DC0",
            List.of("127.0.0.1"),
            1111,
            new Supplier<SchemaChangeListener>() {
              @Override
              public SchemaChangeListener get() {
                return null;
              }
            });

    /*
         String applicationName,
         DatabaseType databaseType,
         String localDatacenter,
         List<String> cassandraEndPoints,
         Integer cassandraPort,
         Supplier<SchemaChangeListener> schemaChangeListenerSupplier) {

    */
  }

  @BeforeEach
  void beforeEach() {
    // Test env-var for sanity checks
    environmentVariables.set("env-debug", "true");

    // and then actual overrides
    // NOTE: ENV var name mangling is done so that
    //
    // * 1 underscore (_) represents dot "."
    // * 2 underscores (_) represents hyphen "-"
    // * 3 underscores (_) represents underscore "_"
    final String PREFIX = "CONFIG_FORCE_datastax__java__driver_";
    environmentVariables.set(PREFIX + "basic_session__name", TENANT_OVERRIDE);
    environmentVariables.set(PREFIX + "basic_request_consistency", CONSISTENCY_OVERRIDE);
  }

  // Sanity check for Env var overridability (outside of config loading)
  @Test
  void sanityCheckEnvVarOverridability() {
    assertThat(System.getenv("env-debug")).isEqualTo("true");
  }

  // Uncomment to see overrides
  // @Test
  void testEnvVarOverridesDEBUG() {
    Config config = ConfigFactory.systemEnvironmentOverrides();
    System.err.println("Config -> " + config.root().render());
  }

  // Tests for override via ENV variables over defaults
  @Test
  void testEnvVarOverrides() {

    DriverConfigLoader loader =
        DriverConfigLoader.programmaticBuilder()
            .withString(DefaultDriverOption.SESSION_NAME, DEFAULT_SESSION)
            .build();
    verifyTenantAndConsistency(loader.getInitialConfig(), TENANT_OVERRIDE, CONSISTENCY_OVERRIDE);
  }

  private void verifyTenantAndConsistency(
      DriverConfig config, String expTenant, String expConsistency) {
    System.err.println("CONFIG");
    for (Map.Entry<String, Object> entry : config.getDefaultProfile().entrySet()) {
      System.err.println(" '" + entry.getKey() + "' -> " + entry.getValue());
    }
    assertThat(config.getDefaultProfile().getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .isEqualTo(expConsistency);
    assertThat(config.getDefaultProfile().getString(DefaultDriverOption.SESSION_NAME))
        .isEqualTo(expTenant);
  }
}
