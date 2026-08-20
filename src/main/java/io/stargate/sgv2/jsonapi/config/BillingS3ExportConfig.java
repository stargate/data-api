package io.stargate.sgv2.jsonapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;
import java.util.Optional;

/** Configuration for the billing S3 export (see BillingS3HandlerInstaller). */
@ConfigMapping(prefix = "stargate.jsonapi.billing.s3")
public interface BillingS3ExportConfig {

  /** Master switch: when false the export handler is never installed. */
  @WithDefault("false")
  boolean enabled();

  /** S3 bucket name */
  Optional<String> bucket();

  /** S3 bucket region */
  Optional<String> bucketRegion();

  /** Only for non-AWS S3 endpoints (e.g. S3Mock in tests). */
  Optional<String> endpointOverride();

  /** Line-count seal: a buffered batch is shipped once it holds this many events. */
  @WithDefault("50")
  int maxEvents();

  /** UTF-8 NDJSON byte seal; a batch may exceed it by one whole event. */
  @WithDefault("2097152")
  long maxBytes();

  /** Age flush period: buffered events are shipped at least this often, sealed or not. */
  @WithDefault("PT30S")
  Duration maxAge();

  /** Bound on buffered events; beyond it new lines are dropped. */
  @WithDefault("10000")
  int queueCapacity();

  /** Max concurrent S3 PUTs. */
  @WithDefault("4")
  int uploadConcurrency();

  /** Budget for draining the buffer at shutdown; keep below the pod termination grace period. */
  @WithDefault("PT20S")
  Duration shutdownTimeout();
}
