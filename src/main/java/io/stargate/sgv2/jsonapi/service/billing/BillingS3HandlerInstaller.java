package io.stargate.sgv2.jsonapi.service.billing;

import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.stargate.sgv2.jsonapi.config.BillingS3ExportConfig;
import io.stargate.sgv2.jsonapi.metrics.BatchedLogBufferMetrics;
import io.stargate.sgv2.jsonapi.metrics.BatchedLogUploaderMetrics;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.util.logging.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * <p>Attaches a {@link BillingS3LogHandler} to the {@code billing.events} logger at startup
 * (when {@link BillingS3ExportConfig#enabled()} is {@code true}) and removes + closes it on
 * shutdown for a graceful drain.
 *
 */
@ApplicationScoped
public class BillingS3HandlerInstaller {

  private static final org.slf4j.Logger LOGGER =  LoggerFactory.getLogger(BillingS3HandlerInstaller.class);

  private static final String METRICS_PREFIX ="billing";
  private static final String BILLING_LOGGER_NAME = "billing.events";

  private final BillingS3ExportConfig config;
  private final MeterRegistry meterRegistry;

  private volatile BillingS3LogHandler handler;

  @Inject
  public BillingS3HandlerInstaller(BillingS3ExportConfig config, MeterRegistry meterRegistry) {
    this.config = config;
    this.meterRegistry = meterRegistry;
  }

  void onStart(@Observes StartupEvent event) {

    if (!config.enabled()) {
      LOGGER.info("Billing S3 export disabled");
      return;
    }

    // Fail-loud: invalid billing S3 config throws here, aborting application startup.
    var uploader = S3BatchedLogUploader.create(
            config.region(),
            config.bucket(),
            config.endpointOverride().orElse(null),
            new BatchedLogUploaderMetrics(meterRegistry, METRICS_PREFIX));

    var buffer= new BatchedLogBuffer(
            config.maxEventsPerBatch(),
            config.maxBytesPerBatch(),
            config.maxAge(),
            config.queueCapacity(),
            new BatchedLogBufferMetrics(meterRegistry, METRICS_PREFIX));

    this.handler = new BillingS3LogHandler(config, uploader, meterRegistry);

    // TODO: LOGGER NAME SHOULD BE IN CONFIG
    Logger.getLogger(BILLING_LOGGER_NAME).addHandler(this.handler);

    LOGGER.info(
        "Attached billing S3 export handler to logger named {}, bucket={}, region={}, endpointOverride={}",
        BILLING_LOGGER_NAME,
        bucket,
        region,
        config.endpointOverride().orElse(null));
  }

  void onStop(@Observes ShutdownEvent event) {

    if (this.handler == null) {
      return;
    }

    // TODO: XXX WHY DO THIS ?
    Logger.getLogger(BILLING_LOGGER_NAME).removeHandler(this.handler);

    // close() isn't expected to throw, but if it does (e.g. client.close() failing), letting it
    // propagate would disrupt other components' cleanup in Quarkus's shutdown sequence.
    try {
      this.handler.close();
    } catch (Exception e) {
      LOGGER.warn("Error during billing S3 export handler shutdown", e);
    }
  }
}
