package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.stargate.sgv2.jsonapi.config.BillingS3ExportConfig;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BillingS3HandlerInstaller}: install/uninstall symmetry on the {@code
 * billing.events} JUL logger, the disabled path, and fail-loud startup on bad config. Delivery
 * through an installed handler is covered by {@code BillingS3ExportIntegrationTest}.
 */
class BillingS3HandlerInstallerTest {

  private static BillingS3ExportConfig config(boolean enabled, String bucket, String region) {
    BillingS3ExportConfig config = mock(BillingS3ExportConfig.class);
    when(config.enabled()).thenReturn(enabled);
    when(config.bucket()).thenReturn(Optional.ofNullable(bucket));
    when(config.bucketRegion()).thenReturn(Optional.ofNullable(region));
    when(config.endpointOverride()).thenReturn(Optional.empty());
    when(config.maxEvents()).thenReturn(50);
    when(config.maxBytes()).thenReturn(2_097_152L);
    when(config.maxAge()).thenReturn(Duration.ofSeconds(30));
    when(config.queueCapacity()).thenReturn(100);
    when(config.uploadConcurrency()).thenReturn(2);
    when(config.shutdownTimeout()).thenReturn(Duration.ofSeconds(1));
    return config;
  }

  private static long installedHandlers() {
    return Arrays.stream(
            Logger.getLogger(BillingS3HandlerInstaller.BILLING_LOGGER_NAME).getHandlers())
        .filter(BillingS3LogHandler.class::isInstance)
        .count();
  }

  @Test
  void disabledConfigInstallsNothing() {
    var installer =
        new BillingS3HandlerInstaller(config(false, null, null), new SimpleMeterRegistry());

    installer.onStart(new StartupEvent());

    assertThat(installedHandlers()).isZero();
    installer.onStop(new ShutdownEvent()); // must be a safe no-op without an installed handler
  }

  @Test
  void missingBucketFailsStartupLoudly() {
    var installer =
        new BillingS3HandlerInstaller(config(true, null, "us-east-1"), new SimpleMeterRegistry());

    assertThatThrownBy(() -> installer.onStart(new StartupEvent()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("bucket");
    assertThat(installedHandlers()).isZero();
  }

  @Test
  void installsOnStartupAndRemovesAndClosesOnShutdown() {
    var installer =
        new BillingS3HandlerInstaller(
            config(true, "my-bucket", "us-east-1"), new SimpleMeterRegistry());

    installer.onStart(new StartupEvent());
    try {
      assertThat(installedHandlers()).isEqualTo(1);
    } finally {
      installer.onStop(new ShutdownEvent());
    }
    assertThat(installedHandlers()).isZero();
  }
}
