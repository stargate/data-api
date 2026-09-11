package io.stargate.sgv2.jsonapi.service.billing;

import static io.stargate.sgv2.jsonapi.service.billing.BillingS3HandlerInstaller.BILLING_LOGGER_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.stargate.sgv2.jsonapi.config.BillingS3ExportConfig;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BillingS3HandlerInstallerTest {

  private Logger logger;
  private Handler[] saved;

  @BeforeEach
  void saveHandlers() {
    logger = Logger.getLogger(BILLING_LOGGER_NAME);
    saved = logger.getHandlers();
  }

  @AfterEach
  void restoreHandlers() {
    for (var h : logger.getHandlers()) {
      logger.removeHandler(h);
    }
    for (var h : saved) {
      logger.addHandler(h);
    }
  }

  @Test
  void onStartBillingEnabledOthersDisabled() {

    var installer =
        installer(
            Map.of(
                "stargate.jsonapi.billing.s3.enabled",
                "true",
                "stargate.jsonapi.billing.s3.disable-other-handlers",
                "true"),
            null);

    assertHandlers(installer, true, false);
  }

  @Test
  void onStartBillingEnabledOthersEnabled() {

    var installer =
        installer(
            Map.of(
                "stargate.jsonapi.billing.s3.enabled",
                "true",
                "stargate.jsonapi.billing.s3.disable-other-handlers",
                "false"),
            null);

    assertHandlers(installer, true, true);
  }

  @Test
  void onStartBillingDisabledOthersDisabled() {

    var installer =
        installer(
            Map.of(
                "stargate.jsonapi.billing.s3.enabled",
                "false",
                "stargate.jsonapi.billing.s3.disable-other-handlers",
                "true"),
            null);

    // even though disabling others is enabled, billing s3 is disabled so that should not impact
    assertHandlers(installer, false, true);
  }

  @Test
  void onStartBillingDisabledOthersEnabled() {

    var installer =
        installer(
            Map.of(
                "stargate.jsonapi.billing.s3.enabled",
                "false",
                "stargate.jsonapi.billing.s3.disable-other-handlers",
                "false"),
            null);

    // even though disabling others is enabled, billing s3 is disabled so that should not impact
    assertHandlers(installer, false, true);
  }

  // ====================================
  // Scaffold
  // ====================================

  private void assertHandlers(
      BillingS3HandlerInstaller installer, boolean expectS3Handler, boolean expectOtherHandlers) {

    var billingLogger = Logger.getLogger(BILLING_LOGGER_NAME);

    try {
      installer.onStart(new StartupEvent());
      var onStartHandlers = installedHandlers();
      if (expectS3Handler) {
        assertThat(onStartHandlers).as("onStart attached S3 handler").contains(installer.handler());
      }
      if (expectOtherHandlers) {
        assertThat(onStartHandlers.size())
            .as("onStart left other handler in place")
            .isGreaterThanOrEqualTo(expectS3Handler ? 2 : 1);
      } else {
        assertThat(onStartHandlers.size())
            .as("onStart removed other handlers.")
            .isGreaterThanOrEqualTo(expectS3Handler ? 1 : 1);
      }

    } finally {
      installer.onStop(new ShutdownEvent());
    }
    var onStopHandlers = installedHandlers();

    // always expect that the S3 handler is removed.
    assertThat(installedHandlers())
        .as("onStop removes S3 handler")
        .doesNotContain(installer.handler());

    if (expectOtherHandlers) {
      assertThat(onStopHandlers.size())
          .as("onStart left other handler in place")
          .isGreaterThanOrEqualTo(1);
    } else {
      assertThat(onStopHandlers.size()).as("onStart removed all handlers.").isEqualTo(0);
    }
  }

  private static List<Handler> installedHandlers() {
    return Arrays.stream(Logger.getLogger(BILLING_LOGGER_NAME).getHandlers()).toList();
  }

  private static BillingS3HandlerInstaller installer(
      Map<String, String> configOverride, MeterRegistry meterRegistry) {

    var builder = new SmallRyeConfigBuilder().withMapping(BillingS3ExportConfig.class);
    if (configOverride != null) {
      configOverride.forEach(builder::withDefaultValue);
    }
    var config = builder.build().getConfigMapping(BillingS3ExportConfig.class);

    meterRegistry = meterRegistry == null ? new SimpleMeterRegistry() : meterRegistry;

    return new BillingS3HandlerInstaller(config, meterRegistry);
  }
}
