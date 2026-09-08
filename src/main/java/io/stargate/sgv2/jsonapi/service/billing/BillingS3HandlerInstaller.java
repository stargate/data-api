package io.stargate.sgv2.jsonapi.service.billing;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.stargate.sgv2.jsonapi.config.BillingS3ExportConfig;
import io.stargate.sgv2.jsonapi.metrics.BatchedLogBufferMetrics;
import io.stargate.sgv2.jsonapi.metrics.BatchedLogUploaderMetrics;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.util.logging.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates setting up billing to send billing events to S3.
 *
 * <p>
 *
 * <ul>
 *   <li>Encapsulates all the quarkus / CDI injection in here, so the billing classes are not bound
 *       to that approach.
 *   <li>Subscribes to the quarkus lifecycle for startup and shutdown to configure billing upload,
 *       get it's uploading thread running, and then close it so we flush when shutting down.
 * </ul>
 */
@ApplicationScoped
public class BillingS3HandlerInstaller {

  private static final org.slf4j.Logger LOGGER =
      LoggerFactory.getLogger(BillingS3HandlerInstaller.class);

  private static final String METRICS_PREFIX = "billing";
  // TODO: MOVE , this is duplicated
  public static final String BILLING_LOGGER_NAME = "billing.events";

  private final BillingS3ExportConfig config;
  private final MeterRegistry meterRegistry;

  private volatile BillingUploadingLogHandler handler;

  @VisibleForTesting
  BillingUploadingLogHandler handler() {
    return this.handler;
  }

  @Inject
  public BillingS3HandlerInstaller(BillingS3ExportConfig config, MeterRegistry meterRegistry) {
    this.config = config;
    this.meterRegistry = meterRegistry;
  }

  void onStart(@Observes StartupEvent event) {

    if (!config.enabled()) {
      LOGGER.info("onStart() - S3 export disabled");
      return;
    }

    LOGGER.info("onStart() - S3 export enabled");

    var uploader =
        S3BatchedLogUploader.create(
            config.region(),
            config.bucket(),
            config.endpointOverride().orElse(null),
            config.s3PathPrefix(),
            config.s3CallAttemptTimeout(),
            config.s3TotalCallTimeout(),
            config.s3RetryMode(),
            new BatchedLogUploaderMetrics(meterRegistry, METRICS_PREFIX));
    LOGGER.info("onStart() - using uploader: {}", uploader);

    var buffer =
        new BatchedLogBuffer(
            config.bufferMaxBatchSize(),
            config.bufferMaxBatchBytes(),
            config.bufferMaxBatchAge(),
            config.queueCapacity(),
            new BatchedLogBufferMetrics(meterRegistry, METRICS_PREFIX));
    LOGGER.info("onStart() - using log buffer: {}", buffer);

    this.handler =
        new BillingUploadingLogHandler(
            buffer,
            uploader,
            config.handlerSleepDuration(),
            config.handlerUploadSafetyDeadline(),
            config.handlerUploadShutdownDeadline());
    LOGGER.info("onStart() - using handler: {}", handler);

    var billingLogger = Logger.getLogger(BILLING_LOGGER_NAME);
    if (config.disableOtherHandlers()) {
      LOGGER.info("onStart() - removing existing log handlers");
      for (var existing : billingLogger.getHandlers()) {
        LOGGER.info("onStart() - removing existing log handler. existing:{} ", existing);
        billingLogger.removeHandler(existing);
      }
    } else {
      LOGGER.info("onStart() - leaving existing log handlers");
    }

    billingLogger.addHandler(this.handler);
    LOGGER.info(
        "onStart() - attached log handler to logger. BILLING_LOGGER_NAME: {}", BILLING_LOGGER_NAME);

    Infrastructure.getDefaultWorkerPool()
        .execute(
            () -> {
              LOGGER.info(
                  "onStart() - on worked pool thread, calling handler.startUploading() on this thread");
              this.handler.startUploading();
            });
  }

  void onStop(@Observes ShutdownEvent event) {

    if (this.handler == null) {
      LOGGER.info("onStop() - handler was null, nothing to close");
      return;
    }

    Logger.getLogger(BILLING_LOGGER_NAME).removeHandler(this.handler);
    LOGGER.info(
        "onStop() - handler removed from logger. BILLING_LOGGER_NAME:{}", BILLING_LOGGER_NAME);

    // close() isn't expected to throw, but if it does (e.g. client.close() failing), letting it
    // propagate would disrupt other components' cleanup in Quarkus's shutdown sequence.
    try {
      LOGGER.debug("onStop() - calling handler.close()");
      this.handler.close();
      LOGGER.info("onStop() - handler was closed without error");
    } catch (Exception e) {
      LOGGER.warn("onStop() - error calling handler.close(), swallowing", e);
    }
  }
}
