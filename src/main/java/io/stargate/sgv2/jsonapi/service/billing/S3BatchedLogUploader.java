package io.stargate.sgv2.jsonapi.service.billing;

import com.google.common.annotations.VisibleForTesting;
import io.smallrye.mutiny.Uni;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.concurrent.CompletionException;

import io.stargate.sgv2.jsonapi.metrics.BillingMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/** Uploads sealed billing batches to S3 as NDJSON objects under time-partitioned keys. */
public class S3BatchedLogUploader implements AsyncBatchedLogUploader {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3BatchedLogUploader.class);

  // S3 destination formatting
  private static final String PATH_PREFIX = "data-api";
  private static final String CONTENT_TYPE_NDJSON = "application/x-ndjson";
  private static final DateTimeFormatter OBJECT_KEY_FORMAT =
      DateTimeFormatter.ofPattern("yyyy/MM/dd/HH/mm").withZone(ZoneOffset.UTC);

  // Bound every PUT so a hung connection can neither pin an upload slot indefinitely nor stall
  // the shutdown drain. Retries stay inside the SDK's built-in default policy (bounded attempts,
  // jittered throttle-aware backoff).
  private static final Duration API_CALL_ATTEMPT_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration API_CALL_TIMEOUT = Duration.ofSeconds(30);

  private final S3AsyncClient client;
  private final String bucket;
  private final BillingMetrics billingMetrics;

  @VisibleForTesting
  S3BatchedLogUploader(S3AsyncClient client, String bucket, BillingMetrics billingMetrics) {
    this.client = client;
    this.bucket = bucket;
    this.billingMetrics = billingMetrics;
  }

  /**
   * Creates a new instance
   *
   * @param region
   * @param bucket
   * @param endpointOverride
   * @return
   */
  public static S3BatchedLogUploader create(String region, String bucket, String endpointOverride, BillingMetrics billingMetrics) {

    if (region == null || region.isBlank()) {
      throw new IllegalArgumentException("region must be set");
    }
    if (bucket == null || bucket.isBlank()) {
      throw new IllegalArgumentException("bucket must be set");
    }

    // Credentials resolve from the SDK's default provider chain (env vars, web-identity/OIDC
    // token, instance/container roles), left implicit so the client owns — and closes — the
    // provider. This transparently supports federated (AssumeRoleWithWebIdentity) and
    // cross-account access: the bucket may live in a different account (per IAM + bucket
    // policy); its region is set via .region().
    var builder =
        S3AsyncClient.builder()
            .region(Region.of(region))
            .overrideConfiguration(
                config ->
                    config
                        .apiCallAttemptTimeout(API_CALL_ATTEMPT_TIMEOUT)
                        .apiCallTimeout(API_CALL_TIMEOUT));

    // Real AWS S3 needs no endpoint: the SDK endpoint rules (s3 SDK's DefaultS3EndpointProvider)
    // derive https://<bucket>.s3.<region>.amazonaws.com from region + partition dnsSuffix.
    // An override is only for a non-AWS S3 (S3Mock in tests): it bypasses those rules and forces
    // path-style, since a localhost host can't virtual-host the bucket as a subdomain.
    if (endpointOverride != null) {
      builder.endpointOverride(URI.create(endpointOverride)).forcePathStyle(true);
    }

    return new S3BatchedLogUploader(builder.build(), bucket);
  }

  @Override
  public Uni<UploadResult> upload(BatchedLogBuffer.Batch batch) {

    Objects.requireNonNull(batch, "batch must not be null");

    var key = objectKey(batch);
    var body = objectContent(batch);

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "upload() - starting to upload batch, batch:({}), S3.bucket:{}, S3.key:{}",
          batch.description(),
          bucket,
          key);
    }

    // No .retry() here: unconfigured, S3AsyncClient already retries (default LegacyRetryStrategy —
    // 3 retries / 4 attempts). See
    // https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/retry-strategy.html
    // and see https://github.com/aws/aws-sdk-java-v2/issues/6987 for future change.

    return Uni.createFrom()
        .completionStage(
            () ->
                client.putObject(
                    PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .contentType(CONTENT_TYPE_NDJSON)
                        .build(),
                    AsyncRequestBody.fromBytes(body)))
        .onItemOrFailure()
        .transform(
            (resp, failure) -> {
              var success = failure == null;
              var cause = (failure instanceof CompletionException) ? failure.getCause() : failure;
              var requestId = (cause instanceof AwsServiceException ase) ? ase.requestId() : null;

              if (!success) {
                billingMetrics.recordBatchFailed(batch.size());
                LOGGER.error(
                    "upload() - error uploading billing to S3, batch:({}), bytes.length:{}, requestId:{}, S3.bucket:{}, S3.key:{}",
                    batch.description(),
                    body.length,
                    requestId,
                    bucket,
                    key,
                    cause);
              } else {
                billingMetrics.recordBatchDelivered(batch.size());
                LOGGER.debug(
                    "upload() - success uploading billing to S3, batch:({}), bytes.length:{}, eTag:{}, status:{}, requestId={}, S3.bucket:{}, S3.key:{}",
                    batch.description(),
                    body.length,
                    resp.eTag(),
                    resp.sdkHttpResponse().statusCode(),
                    resp.responseMetadata().requestId(),
                        bucket,
                        key);
              }
              return new  UploadResult(success, failure, batch);
            });
  }

  @Override
  public void close() {
    client.close();
  }

  private static String objectKey(BatchedLogBuffer.Batch batch) {

    var objectKey =
        PATH_PREFIX
            + "/"
            + OBJECT_KEY_FORMAT.format(batch.oldestEventAt())
            + "/"
            + batch.id()
            + ".jsonl";

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("objectKey() - batch.id:{} , objectKey:{}", batch.id(), objectKey);
    }
    return objectKey;
  }

  private static byte[] objectContent(BatchedLogBuffer.Batch batch) {

    StringBuilder sb = new StringBuilder();
    for (String line : batch.lines()) {
      sb.append(line).append('\n');
    }
    var bytes = sb.toString().getBytes(StandardCharsets.UTF_8);

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("objectContent() - batch.id:{} , bytes.length:{}", batch.id(), bytes.length);
    }
    return bytes;
  }
}
