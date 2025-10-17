package io.stargate.sgv2.jsonapi.service.cqldriver;

import static com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader.DEFAULT_ROOT_PATH;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverOption;
import org.junit.jupiter.api.Test;

public class CqlDriverConfigLoadTests {
  private final String DEFAULT_SESSION = "tenant-none";
  // from Driver's "reference.conf"
  private final String DEFAULT_CONSISTENCY = "LOCAL_ONE";

  // Test for values from Driver's "reference.conf", explicit session (tenant)
  @Test
  void testDefaultsNoOverrides() {
    DriverConfigLoader loader =
        DriverConfigLoader.programmaticBuilder()
            .withString(DefaultDriverOption.SESSION_NAME, DEFAULT_SESSION)
            .build();
    verifyTenantAndConsistency(loader.getInitialConfig(), DEFAULT_SESSION, DEFAULT_CONSISTENCY);
  }

  @Test
  void testSystemPropOverrides() {
    final String TENANT_OVERRIDE = "tenant-X";
    final String CONSISTENCY_OVERRIDE = "QUORUM";

    try {
      setSystemProperty(DefaultDriverOption.SESSION_NAME, TENANT_OVERRIDE);
      setSystemProperty(DefaultDriverOption.REQUEST_CONSISTENCY, CONSISTENCY_OVERRIDE);
      DriverConfigLoader loader =
          DriverConfigLoader.programmaticBuilder()
              .withString(DefaultDriverOption.SESSION_NAME, DEFAULT_SESSION)
              .build();
      verifyTenantAndConsistency(loader.getInitialConfig(), TENANT_OVERRIDE, CONSISTENCY_OVERRIDE);
    } finally {
      clearSystemProperty(DefaultDriverOption.SESSION_NAME);
      clearSystemProperty(DefaultDriverOption.REQUEST_CONSISTENCY);
    }
  }

  private void verifyTenantAndConsistency(
      DriverConfig config, String expTenant, String expConsistency) {
    assertThat(config.getDefaultProfile().getString(DefaultDriverOption.SESSION_NAME))
        .isEqualTo(expTenant);
    assertThat(config.getDefaultProfile().getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .isEqualTo(expConsistency);
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
