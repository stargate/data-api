package io.stargate.sgv2.jsonapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;
import java.util.Optional;
import software.amazon.awssdk.core.retry.RetryMode;

/** Configuration for the billing S3 export (see BillingS3HandlerInstaller). */
@ConfigMapping(prefix = "stargate.jsonapi.billing.s3")
public interface BillingS3ExportConfig {

  /** when false the export handler is never installed. */
  @WithDefault("false")
  boolean enabled();

  /** S3 bucket region */
  @WithDefault("us-east-2")
  String region();

  /** S3 bucket name */
  @WithDefault("serverless-usage-dev")
  String bucket();

  @WithDefault("data-api")
  String s3PathPrefix();

  @WithDefault("PT10S")
  Duration s3CallAttemptTimeout();

  @WithDefault("PT30S")
  Duration s3TotalCallTimeout();

  @WithDefault("STANDARD")
  RetryMode s3RetryMode();

  /**
   * Only for non-AWS S3 endpoints (e.g. S3Mock in tests). TODO: XXX EXPLAIN WHAT THIS SHOULD SET
   * SET TO
   */
  Optional<String> endpointOverride();

  /** */
  @WithDefault("5000")
  int bufferMaxBatchSize();

  /**
   * Max bytes to include in a batch, NOTE: if a single event is bigger than this it will be sent in
   * a batch still. 2097152 == 2 MB
   */
  @WithDefault("2097152")
  long bufferMaxBatchBytes();

  /** Age flush period: buffered events are shipped at least this often */
  @WithDefault("PT60S")
  Duration bufferMaxBatchAge();

  /**
   * Bound on buffered events; beyond it new lines are dropped. 5,000 events per batch, so set to
   * 5,000 * 10 = 50,000 up to 20 MB
   */
  @WithDefault("50000")
  int queueCapacity();

  @WithDefault("PT60S")
  Duration handlerSleepDuration();

  @WithDefault("PT30S")
  Duration handlerUploadSafetyDeadline();

  @WithDefault("PT30S")
  Duration handlerUploadShutdownDeadline();
}
