package io.stargate.sgv2.jsonapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.stargate.sgv2.jsonapi.service.billing.BillingS3Lifecycle;
import io.stargate.sgv2.jsonapi.service.billing.BillingUploadingLogHandler;
import java.time.Duration;
import java.util.Optional;
import software.amazon.awssdk.core.retry.RetryMode;

/**
 * Configuration for the billing S3 export (see {@link BillingS3Lifecycle}).
 *
 * <p>See also {@link BillingConfig}
 */
@ConfigMapping(prefix = "stargate.jsonapi.billing.s3")
public interface BillingS3UploadConfig {

  /** When <code>true</code> billing events are sent to S3 using the config below. */
  @WithDefault("false")
  boolean enabled();

  /**
   * When <code>true</code> adding the S3 handler for logging at run time removes any other handler
   * the logger has installed. <bold>NOTE:</bold> is only applied IF S3 is enabled
   */
  @WithDefault("true")
  boolean disableOtherHandlers();

  /** Required, S3 bucket region to send billing events to. */
  @WithDefault("us-east-2")
  String region();

  /** Required, S3 bucket name to send billing events to. */
  @WithDefault("serverless-usage")
  String bucket();

  /**
   * Required, path prefix that billing events are added under. See {@link
   * io.stargate.sgv2.jsonapi.service.billing.S3BatchedLogUploader} for the full path.
   */
  @WithDefault("data-api")
  String s3PathPrefix();

  /**
   * When uploading to S3, the timeout to use fo a single upload request. See {@link
   * io.stargate.sgv2.jsonapi.service.billing.S3BatchedLogUploader}
   */
  @WithDefault("PT10S")
  Duration s3CallAttemptTimeout();

  /**
   * When uploading to S3, the total time all attempts to upload may take. See {@link
   * io.stargate.sgv2.jsonapi.service.billing.S3BatchedLogUploader}
   */
  @WithDefault("PT30S")
  Duration s3TotalCallTimeout();

  /** See S3 client {@link RetryMode} */
  @WithDefault("STANDARD")
  RetryMode s3RetryMode();

  /** Overrides the path for S2 to be used in testing such as S3Mock in tests. */
  Optional<String> endpointOverride();

  /**
   * Maximum number of events to put into a single batch of buffered billing events. When the buffer
   * has this many events a new batch is created. See {@link
   * io.stargate.sgv2.jsonapi.service.billing.BatchedLogBuffer}
   */
  @WithDefault("5000")
  int bufferMaxBatchSize();

  /**
   * Max bytes to include in a batch, NOTE: if a single event is bigger than this it will be sent in
   * a batch still. 2097152 == 2 MB which is approx 5,000 events at 400 Bytes each. When the buffer
   * has this many bytes a new batch is created. See {@link
   * io.stargate.sgv2.jsonapi.service.billing.BatchedLogBuffer}
   */
  @WithDefault("2097152")
  long bufferMaxBatchBytes();

  /**
   * Maximum age the first (head) billing event in the buffer can get to before triggering a batch
   * being created. See {@link io.stargate.sgv2.jsonapi.service.billing.BatchedLogBuffer}
   */
  @WithDefault("PT60S")
  Duration bufferMaxBatchAge();

  /**
   * Maximum number of events that can be held in the buffer before events are dropped. See {@link
   * io.stargate.sgv2.jsonapi.service.billing.BatchedLogBuffer} and {@link
   * io.stargate.sgv2.jsonapi.service.billing.BillingUploadingLogHandler}
   */
  @WithDefault("50000")
  int bufferCapacity();

  /**
   * Length of time the billing log handler should sleep before checking if a new batch(s) is
   * available from the logging buffer. See {@link
   * io.stargate.sgv2.jsonapi.service.billing.BillingUploadingLogHandler}
   */
  @WithDefault("PT60S")
  Duration handlerSleepDuration();

  /**
   * Length of time the thread doing uploading from the logging handler will wait for the S3 upload
   * to complete. NOTE that while there are timeouts etc above for S3, they are passed into the AWS
   * client. This is the timeout the uploading thread uses incase the upload stalls. See {@link
   * io.stargate.sgv2.jsonapi.service.billing.BillingUploadingLogHandler}
   */
  @WithDefault("PT30S")
  Duration handlerUploadSafetyDeadline();

  /**
   * Length of time the thread used to call {@link BillingUploadingLogHandler#close()} will wait
   * forthe uploading thread to have completed uploading all remaining events. See {@link
   * io.stargate.sgv2.jsonapi.service.billing.BillingUploadingLogHandler}
   */
  @WithDefault("PT30S")
  Duration handlerUploadShutdownDeadline();
}
