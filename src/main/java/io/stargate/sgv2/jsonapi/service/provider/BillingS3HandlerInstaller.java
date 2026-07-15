package io.stargate.sgv2.jsonapi.service.provider;

import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.stargate.sgv2.jsonapi.config.BillingS3ExportConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.util.logging.Logger;
import org.slf4j.LoggerFactory;

/**
 * Attaches a {@link BillingS3LogHandler} to the {@code billing.events} JUL logger at startup (when
 * {@link BillingS3ExportConfig#enabled()} is {@code true}) and removes + closes it on shutdown for
 * a graceful drain.
 *
 * <p>Done programmatically because Quarkus config can't express it: a category's {@code handlers}
 * list can only reference Quarkus's built-in handler types (console/file/syslog/socket), not a
 * custom {@link java.util.logging.Handler} class. The one config-driven alternative — a discovered
 * {@code @Produces Handler} bean — attaches to the <i>root</i> logger, but {@code billing.events}
 * is {@code use-parent-handlers: false} and we want delivery scoped to exactly that category. The
 * {@link StartupEvent} observer runs after Quarkus has applied its logging config, so the
 * registration sticks.
 */
@ApplicationScoped
public class BillingS3HandlerInstaller {

  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(BillingS3HandlerInstaller.class);

  static final String BILLING_LOGGER_NAME = "billing.events";

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
      LOG.debug("Billing S3 export disabled (stargate.jsonapi.billing.s3.enabled=false)");
      return;
    }

    var region = config.bucketRegion().orElse(null);
    var bucket = config.bucket().orElse(null);

    // Fail-loud: invalid billing S3 config throws here, aborting application startup.
    var uploader = S3BatchUploader.create(region, bucket, config.endpointOverride());

    this.handler = new BillingS3LogHandler(config, uploader, meterRegistry);
    Logger.getLogger(BILLING_LOGGER_NAME).addHandler(this.handler);

    LOG.info(
        "Installed billing S3 export handler on '{}' → bucket '{}' (region '{}', endpointOverride={})",
        BILLING_LOGGER_NAME,
        bucket,
        region,
        config.endpointOverride().orElse(null));
  }

  void onStop(@Observes ShutdownEvent event) {
    if (this.handler == null) {
      return;
    }
    Logger.getLogger(BILLING_LOGGER_NAME).removeHandler(this.handler);
    try {
      this.handler.close();
    } catch (Exception e) {
      LOG.warn("Error during billing S3 export handler shutdown", e);
    }
  }
}
