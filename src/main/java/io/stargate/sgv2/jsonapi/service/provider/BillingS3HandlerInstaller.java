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
 * Wires a {@link BillingS3LogHandler} onto the {@code billing.events} logger at startup when {@link
 * BillingS3ExportConfig#enabled()} is {@code true}, and removes/closes it on shutdown for a
 * graceful drain.
 *
 * <p>We attach the handler directly to the {@code billing.events} JUL logger rather than relying on
 * Quarkus's discovered-{@code Handler}-bean mechanism: discovered handler beans are attached to the
 * <i>root</i> logger, but {@code billing.events} is configured {@code use-parent-handlers: false}
 * (so it would never feed a root handler) and we want this handler scoped to exactly that category.
 * Adding the handler here in a {@link StartupEvent} observer runs after Quarkus has applied its
 * logging configuration, so the registration sticks; we keep a strong reference to the logger so it
 * (and our handler) cannot be GC'd.
 *
 * <p>Injecting {@link BillingS3ExportConfig} also pins it as a SmallRye {@code @ConfigMapping} bean
 * so Quarkus ARC does not drop it at build time (see {@link
 * io.stargate.sgv2.jsonapi.JsonApiStartUp} for the same pattern with {@code BillingConfig}).
 */
@ApplicationScoped
public class BillingS3HandlerInstaller {

  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(BillingS3HandlerInstaller.class);

  static final String BILLING_LOGGER_NAME = "billing.events";

  private final BillingS3ExportConfig config;
  private final MeterRegistry meterRegistry;

  // Strong references so the configured logger (and the handler we add to it) are not collected,
  // and so we can detach cleanly on shutdown.
  private volatile Logger billingLogger;
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

    String bucket = config.bucket().filter(s -> !s.isBlank()).orElse(null);
    String region = config.bucketRegion().filter(s -> !s.isBlank()).orElse(null);
    if (bucket == null || region == null) {
      LOG.error(
          "Billing S3 export is enabled but bucket/region are not fully configured (bucket={},"
              + " region={}); handler NOT installed. Billing events continue to the console only.",
          config.bucket().orElse("<unset>"),
          config.bucketRegion().orElse("<unset>"));
      return;
    }

    try {
      S3BatchUploader uploader = S3BatchUploader.create(region, bucket, config.endpointOverride());
      BillingS3LogHandler newHandler = new BillingS3LogHandler(config, uploader, meterRegistry);

      Logger logger = Logger.getLogger(BILLING_LOGGER_NAME);
      logger.addHandler(newHandler);

      this.handler = newHandler;
      this.billingLogger = logger;
      LOG.info(
          "Installed billing S3 export handler on '{}' → bucket '{}' (region '{}', endpointOverride={})",
          BILLING_LOGGER_NAME,
          bucket,
          region,
          config.endpointOverride().orElse("<none>"));
    } catch (Exception e) {
      LOG.error(
          "Failed to install billing S3 export handler; billing events continue to the console only",
          e);
    }
  }

  void onStop(@Observes ShutdownEvent event) {
    BillingS3LogHandler current = this.handler;
    if (current == null) {
      return;
    }
    if (billingLogger != null) {
      billingLogger.removeHandler(current);
    }
    try {
      current.close(); // drains remaining batches
    } catch (Exception e) {
      LOG.warn("Error during billing S3 export handler shutdown", e);
    }
  }
}
