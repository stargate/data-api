package io.stargate.sgv2.jsonapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;
import java.util.Optional;

/**
 * Configuration for exporting {@code billing.events} log lines to S3 as NDJSON {@code .jsonl}
 * objects. Consumed by {@link io.stargate.sgv2.jsonapi.service.provider.BillingS3HandlerInstaller}
 * which, when {@link #enabled()} is {@code true}, attaches a {@link
 * io.stargate.sgv2.jsonapi.service.provider.BillingS3LogHandler} to the {@code billing.events}
 * logger.
 *
 * <p>This is a startup-time switch, <b>not</b> a per-request feature flag — it is independent of
 * {@link io.stargate.sgv2.jsonapi.config.feature.ApiFeature#BILLING_EVENTS_LOGGING}. Events only
 * reach the handler when the {@code billing.events} logger is also emitting (i.e. the billing
 * feature is on); this flag then decides whether those lines are additionally shipped to S3. The
 * existing console handler stays attached as a backstop regardless.
 *
 * <p><b>Off by default.</b> When enabled, {@link #bucket()} and {@link #bucketRegion()} are
 * required; if either is missing the handler is not installed (logged as an error) and billing
 * events continue to flow to the console only.
 */
@ConfigMapping(prefix = "stargate.jsonapi.billing.s3")
public interface BillingS3ExportConfig {

  /**
   * Master switch; when {@code false} (default) no handler is installed and no S3 client is built.
   */
  @WithDefault("false")
  boolean enabled();

  /** Target bucket, e.g. {@code serverless-usage-dev}. Required when {@link #enabled()}. */
  Optional<String> bucket();

  /** AWS region of the bucket, e.g. {@code us-east-1}. Required when {@link #enabled()}. */
  Optional<String> bucketRegion();

  /**
   * Endpoint override for the S3 client. Set this to point at a non-AWS S3 (e.g. S3Mock in tests);
   * when present, path-style addressing is forced. SDK resolves the regional AWS endpoint when left
   * empty.
   */
  Optional<String> endpointOverride();

  /** Seal a batch once it holds this many events. */
  @WithDefault("50")
  int maxEvents();

  /** Seal a batch once its NDJSON body reaches this many bytes (~2 MiB default). */
  @WithDefault("2097152")
  long maxBytes();

  /** Seal an open (under-filled) batch once its oldest event is this old (flush interval). */
  @WithDefault("PT30S")
  Duration maxAge();

  /**
   * Capacity of the handler's in-memory hand-off queue. {@link BillingS3LogHandler#publish()}
   * offers lines non-blocking; once the queue is full, further lines are dropped and counted.
   */
  @WithDefault("10000")
  int queueCapacity();

  /** Maximum number of PUT attempts per sealed batch before it is counted as failed. */
  @WithDefault("3")
  int maxUploadAttempts();

  /** Base delay for exponential backoff between PUT attempts ({@code base * 2^(attempt-1)}). */
  @WithDefault("PT0.2S")
  Duration retryBaseBackoff();
}
