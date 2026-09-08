package io.stargate.sgv2.jsonapi.service.billing;

import static io.stargate.sgv2.jsonapi.util.ClassUtils.classSimpleName;

import com.google.common.annotations.VisibleForTesting;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.stargate.sgv2.jsonapi.metrics.BatchedLogUploaderMetrics;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Uploads sealed billing batches to S3 as NDJSON objects under time-partitioned keys. TODO:
 * .requestChecksumCalculation(RequestChecksumCalculation.WHEN_SUPPORTED)
 * .responseChecksumValidation(ResponseChecksumValidation.WHEN_SUPPORTED)
 */
public class S3BatchedLogUploader implements AsyncBatchedLogUploader {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3BatchedLogUploader.class);

  // S3 destination formatting
  //  private static final String PATH_PREFIX = "data-api";
  // this header not defined in standard libs
  @VisibleForTesting public static final String CONTENT_TYPE_NDJSON = "application/x-ndjson";
  private static final DateTimeFormatter OBJECT_KEY_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy/MM/dd/HH/mm").withZone(ZoneOffset.UTC);

  //  private static final Duration API_CALL_ATTEMPT_TIMEOUT = Duration.ofSeconds(10);
  //  private static final Duration API_CALL_TIMEOUT = Duration.ofSeconds(30);

  private final S3AsyncClient client;
  private final String region;
  private final String bucket;
  private final String pathPrefix;

  private final BatchedLogUploaderMetrics metrics;

  /**
   * Visible for testing, use the {@link #create(String, String, String, String, Duration, Duration,
   * RetryMode, BatchedLogUploaderMetrics)} to get a new instance.
   */
  @VisibleForTesting
  S3BatchedLogUploader(
      S3AsyncClient client,
      String region,
      String bucket,
      String pathPrefix,
      BatchedLogUploaderMetrics metrics) {
    this.client = client;
    this.region = region;
    this.bucket = bucket;
    this.pathPrefix = pathPrefix;

    this.metrics = metrics;
  }

  /**
   * Creates a new instance
   *
   * @param region
   * @param bucket
   * @param endpointOverride
   * @return
   */
  public static S3BatchedLogUploader create(
      String region,
      String bucket,
      String endpointOverride,
      String pathPrefix,
      Duration s3CallAttemptTimeout,
      Duration s3TotalCallTimeout,
      RetryMode s3RetryMode,
      BatchedLogUploaderMetrics metrics) {

    if (region == null || region.isBlank()) {
      throw new IllegalArgumentException("region must not be null or blank");
    }
    if (bucket == null || bucket.isBlank()) {
      throw new IllegalArgumentException("bucket must not be  null or blank");
    }
    if (pathPrefix == null || pathPrefix.isBlank()) {
      throw new IllegalArgumentException("pathPrefix must not be null or blank");
    }
    Objects.requireNonNull(s3TotalCallTimeout, "s3TotalCallTimeout must not be null");
    Objects.requireNonNull(s3CallAttemptTimeout, "s3CallAttemptTimeout must not be null");
    Objects.requireNonNull(s3RetryMode, "s3RetryMode must not be null");
    Objects.requireNonNull(metrics, "metrics must not be null");

    // Credentials resolve from the SDK's default provider chain (env vars, web-identity/OIDC
    // token, instance/container roles), left implicit so the client owns — and closes — the
    // provider. This transparently supports federated (AssumeRoleWithWebIdentity) and
    // cross-account access: the bucket may live in a different account (per IAM + bucket
    // policy); its region is set via .region().
    LOGGER.info(
        "create() - region:{}, bucket:{}, pathPrefix:{}, s3TotalCallTimeout:{}, s3CallAttemptTimeout:{}, s3RetryMode:{}",
        region,
        bucket,
        pathPrefix,
        s3TotalCallTimeout,
        s3CallAttemptTimeout,
        s3RetryMode);

    var builder =
        S3AsyncClient.builder()
            .region(Region.of(region))
            .overrideConfiguration(
                config ->
                    config
                        .retryStrategy(s3RetryMode)
                        .apiCallAttemptTimeout(s3CallAttemptTimeout)
                        .apiCallTimeout(s3TotalCallTimeout));

    // Real AWS S3 needs no endpoint: the SDK endpoint rules (s3 SDK's DefaultS3EndpointProvider)
    // derive https://<bucket>.s3.<region>.amazonaws.com from region + partition dnsSuffix.
    // An override is only for a non-AWS S3 (S3Mock in tests): it bypasses those rules and forces
    // path-style, since a localhost host can't virtual-host the bucket as a subdomain.
    if (endpointOverride != null) {
      LOGGER.warn(
          "create() - WARNING - using endpointOverride this should only be used in testing.  endpointOverride: {}",
          endpointOverride);
      builder.endpointOverride(URI.create(endpointOverride)).forcePathStyle(Boolean.TRUE);
    }

    return new S3BatchedLogUploader(builder.build(), region, bucket, pathPrefix, metrics);
  }

  @Override
  public Uni<UploadResult> upload(BatchedLogBuffer.Batch batch) {

    Objects.requireNonNull(batch, "batch must not be null");

    var location = objectLocation(batch);
    var body = objectContent(batch);

    LOGGER.info(
        "upload() - starting to upload batch, batch:{}, location:{}, body.size:{}",
        batch,
        location,
        body.length);

    // retry and timeout are set when we created the client.

    var putRequest =
        PutObjectRequest.builder()
            .bucket(location.bucket())
            .key(location.key())
            .contentType(CONTENT_TYPE_NDJSON)
            .build();
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("upload() - batch:{},  putRequest: {}", batch, putRequest);
    }

    // Call to S3 client comes back on its own worker thread
    // using emit() so following processing happens on a quarkus worker thread
    return Uni.createFrom()
        .completionStage(client.putObject(putRequest, AsyncRequestBody.fromBytes(body)))
        .emitOn(Infrastructure.getDefaultWorkerPool())
        .onItemOrFailure()
        .transform(
            (resp, failure) -> {
              var success = failure == null;
              var cause = (failure instanceof CompletionException) ? failure.getCause() : failure;
              var requestId = (cause instanceof AwsServiceException ase) ? ase.requestId() : null;

              if (!success) {
                metrics.recordBatchFailed(batch);
                LOGGER.error(
                    "upload() - error uploading billing to S3. batch:{}, location:{},  requestId:{}",
                    batch,
                    location,
                    requestId,
                    cause);
              } else {
                metrics.recordBatchDelivered(batch);
                LOGGER.info(
                    "upload() - success uploading billing to S3. batch:{}, location:{}, requestId:{}, eTag:{}, status:{}",
                    batch,
                    location,
                    requestId,
                    resp.eTag(),
                    resp.sdkHttpResponse().statusCode());
              }
              return new UploadResult(batch, failure);
            });
  }

  @Override
  public void close() {
    client.close();
  }

  @Override
  public String toString() {
    return new StringBuilder(classSimpleName(this) + "{")
        .append("region=")
        .append(region)
        .append(", bucket=")
        .append(bucket)
        .append(", pathPrefix=")
        .append(pathPrefix)
        .toString();
  }

  @VisibleForTesting
  S3Location objectLocation(BatchedLogBuffer.Batch batch) {

    var objectKey =
        pathPrefix
            + "/"
            + OBJECT_KEY_FORMATTER.format(batch.oldestEventAt())
            + "/"
            + batch.id()
            + ".jsonl";

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("objectLocation() - batch:{}, objectKey:{}", batch, objectKey);
    }
    return new S3Location(region, bucket, objectKey);
  }

  @VisibleForTesting
  byte[] objectContent(BatchedLogBuffer.Batch batch) {

    StringBuilder sb = new StringBuilder();
    for (String line : batch.lines()) {
      sb.append(line).append('\n');
    }
    var bytes = sb.toString().getBytes(StandardCharsets.UTF_8);

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("objectContent() - batch:{}, bytes.length:{}", batch, bytes.length);
    }
    return bytes;
  }

  @VisibleForTesting
  record S3Location(String region, String bucket, String key) {}
}
