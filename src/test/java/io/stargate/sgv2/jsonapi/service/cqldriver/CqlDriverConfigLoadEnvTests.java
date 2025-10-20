package io.stargate.sgv2.jsonapi.service.cqldriver;

import static com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader.DEFAULT_ROOT_PATH;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import java.util.List;
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

  private static final String TENANT_ENV_OVERRIDE = "tenant-ENV";
  private static final String CONSISTENCY_ENV_OVERRIDE = "ANY";

  @SystemStub private EnvironmentVariables environmentVariables;

  @BeforeAll
  static void initializeSessionFactory() {
    // Important! CqlSessionFactory sets System property for considering
    // Env var overrides; must be called before tests. So just constructs
    // a factory instance to force classloading; not used for anything
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
  }

  @BeforeEach
  void beforeEach() {
    // Test env-var for sanity checks (to ensure tests set up envvars as expected)
    environmentVariables.set("env-debug", "true");

    // and then actual overrides
    // NOTE: ENV var name mangling is done so that
    //
    // * 1 underscore (_) represents dot "."
    // * 2 underscores (_) represents hyphen "-"
    // * 3 underscores (_) represents underscore "_"
    final String PREFIX_TYPESAFE = "CONFIG_FORCE_";
    final String FULL_PREFIX = PREFIX_TYPESAFE + "datastax__java__driver_";

    environmentVariables.set(FULL_PREFIX + "basic_session__name", TENANT_ENV_OVERRIDE);
    environmentVariables.set(FULL_PREFIX + "basic_request_consistency", CONSISTENCY_ENV_OVERRIDE);
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
    verifyTenantAndConsistency(
        loader.getInitialConfig(), TENANT_ENV_OVERRIDE, CONSISTENCY_ENV_OVERRIDE);
  }

  // Tests for override via ENV variables AND sysprops over defaults -- env vars
  // documented to have precedence
  @Test
  void testEnvVarAndSysPropsOverrides() {
    final String TENANT_SYSPROP_OVERRIDE = "tenant-sysprop";
    final String CONSISTENCY_SYSPROP_OVERRIDE = "QUORUM";
    try {
      setSystemProperty(DefaultDriverOption.SESSION_NAME, TENANT_SYSPROP_OVERRIDE);
      setSystemProperty(DefaultDriverOption.REQUEST_CONSISTENCY, CONSISTENCY_SYSPROP_OVERRIDE);

      DriverConfigLoader loader =
          DriverConfigLoader.programmaticBuilder()
              .withString(DefaultDriverOption.SESSION_NAME, DEFAULT_SESSION)
              .build();
      // ENV vars should override everything else
      verifyTenantAndConsistency(
          loader.getInitialConfig(), TENANT_ENV_OVERRIDE, CONSISTENCY_ENV_OVERRIDE);
    } finally {
      clearSystemProperty(DefaultDriverOption.SESSION_NAME);
      clearSystemProperty(DefaultDriverOption.REQUEST_CONSISTENCY);
    }
  }

  private void verifyTenantAndConsistency(
      DriverConfig config, String expTenant, String expConsistency) {
    /*
    System.err.println("CONFIG");
    for (Map.Entry<String, Object> entry : config.getDefaultProfile().entrySet()) {
      System.err.println(" '" + entry.getKey() + "' -> " + entry.getValue());
    }
       */
    assertThat(config.getDefaultProfile().getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .isEqualTo(expConsistency);
    assertThat(config.getDefaultProfile().getString(DefaultDriverOption.SESSION_NAME))
        .isEqualTo(expTenant);
  }

  private static void clearSystemProperty(DriverOption option) {
    System.clearProperty(path(option));
  }

  private static void setSystemProperty(DriverOption option, String value) {
    System.setProperty(path(option), value);
  }

  private static String path(DriverOption option) {
    return DEFAULT_ROOT_PATH + "." + option.getPath();
  }
}
