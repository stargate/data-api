package io.stargate.sgv2.jsonapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;
import java.util.Optional;

/** Configuration for the billing S3 export (see BillingS3HandlerInstaller). */
@ConfigMapping(prefix = "stargate.jsonapi.billing.s3")
public interface BillingS3ExportConfig {

  /** when false the export handler is never installed. */
  @WithDefault("false")
  boolean enabled();

  /** S3 bucket name */
  Optional<String> bucket();

  /** S3 bucket region */
  Optional<String> bucketRegion();

  /**
   * Only for non-AWS S3 endpoints (e.g. S3Mock in tests). TODO: XXX EXPLAIN WHAT THIS SHOULD SET
   * SET TO
   */
  Optional<String> endpointOverride();

  /** */
  @WithDefault("50")
  int maxEventsPerBatch();

  /**
   * Max bytes to include in a batch, NOTE: if a single event is bigger than this it will be sent in
   * a batch still. 2097152 == 2 MB
   */
  @WithDefault("2097152")
  long maxBytesPerBatch();

  /** Age flush period: buffered events are shipped at least this often */
  @WithDefault("PT30S")
  Duration maxAge();

  /** Bound on buffered events; beyond it new lines are dropped. */
  @WithDefault("10000")
  int queueCapacity();

  /** Max concurrent S3 PUTs. */
  @WithDefault("4")
  int uploadConcurrency();

  /**
   * Budget for draining the buffer at shutdown; keep below the pod termination grace period. TODO:
   * XXX WHAT IS THE CURRENT TERMINATION PERIOD ?
   */
  @WithDefault("PT20S")
  Duration shutdownTimeout();
}
