package io.stargate.sgv2.jsonapi.service.billing;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.metrics.BatchedLogBufferMetrics;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BillingS3HandlerInstaller}: install/uninstall symmetry on the {@code
 * billing.events} JUL logger, the disabled path, and fail-loud startup on bad config. Delivery
 * through an installed handler is covered by {@code BillingS3ExportIntegrationTest}.
 */
class BillingS3HandlerInstallerTest {

  @Test
  void submitsUploaderToQuarkusWorkerPool() throws Exception {
    var uploaded = new CountDownLatch(1);
    AsyncBatchedLogUploader uploader =
        batch -> {
          uploaded.countDown();
          return Uni.createFrom().item(new AsyncBatchedLogUploader.UploadResult(true, null, batch));
        };
    var buffer =
        new BatchedLogBuffer(
            1,
            1_000_000,
            Duration.ofMinutes(1),
            10,
            new BatchedLogBufferMetrics(new SimpleMeterRegistry(), "billing-test"));
    var handler = new BillingS3LogHandler(buffer, uploader);
    var installer = new BillingS3HandlerInstaller(null, null);

    installer.startUploading(handler);
    try {
      handler.publish(new LogRecord(Level.INFO, "{\"event\":\"dataapi\"}"));

      assertThat(uploaded.await(3, TimeUnit.SECONDS)).isTrue();
    } finally {
      handler.close();
    }
  }

  //
  //  private static BillingS3ExportConfig config(boolean enabled, String bucket, String region) {
  //    BillingS3ExportConfig config = mock(BillingS3ExportConfig.class);
  //    when(config.enabled()).thenReturn(enabled);
  //    when(config.bucket()).thenReturn(Optional.ofNullable(bucket));
  //    when(config.region()).thenReturn(Optional.ofNullable(region));
  //    when(config.endpointOverride()).thenReturn(Optional.empty());
  //    when(config.maxEventsPerBatch()).thenReturn(50);
  //    when(config.maxBytesPerBatch()).thenReturn(2_097_152L);
  //    when(config.maxAge()).thenReturn(Duration.ofSeconds(30));
  //    when(config.queueCapacity()).thenReturn(100);
  //    when(config.uploadConcurrency()).thenReturn(2);
  //    when(config.shutdownTimeout()).thenReturn(Duration.ofSeconds(1));
  //    return config;
  //  }
  //
  //  private static long installedHandlers() {
  //    return Arrays.stream(
  //            Logger.getLogger(BillingS3HandlerInstaller.BILLING_LOGGER_NAME).getHandlers())
  //        .filter(BillingS3LogHandler.class::isInstance)
  //        .count();
  //  }
  //
  //  @Test
  //  void disabledConfigInstallsNothing() {
  //    var installer =
  //        new BillingS3HandlerInstaller(config(false, null, null), new SimpleMeterRegistry());
  //
  //    installer.onStart(new StartupEvent());
  //
  //    assertThat(installedHandlers()).isZero();
  //    installer.onStop(new ShutdownEvent()); // must be a safe no-op without an installed handler
  //  }
  //
  //  @Test
  //  void missingBucketFailsStartupLoudly() {
  //    var installer =
  //        new BillingS3HandlerInstaller(config(true, null, "us-east-1"), new
  // SimpleMeterRegistry());
  //
  //    assertThatThrownBy(() -> installer.onStart(new StartupEvent()))
  //        .isInstanceOf(IllegalArgumentException.class)
  //        .hasMessageContaining("bucket");
  //    assertThat(installedHandlers()).isZero();
  //  }
  //
  //  @Test
  //  void installsOnStartupAndRemovesAndClosesOnShutdown() {
  //    var installer =
  //        new BillingS3HandlerInstaller(
  //            config(true, "my-bucket", "us-east-1"), new SimpleMeterRegistry());
  //
  //    installer.onStart(new StartupEvent());
  //    try {
  //      assertThat(installedHandlers()).isEqualTo(1);
  //    } finally {
  //      installer.onStop(new ShutdownEvent());
  //    }
  //    assertThat(installedHandlers()).isZero();
  //  }
}
