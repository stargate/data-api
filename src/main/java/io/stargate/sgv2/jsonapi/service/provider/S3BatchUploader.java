package io.stargate.sgv2.jsonapi.service.provider;

import com.google.common.annotations.VisibleForTesting;
import io.smallrye.mutiny.Uni;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/** Uploads sealed billing batches to S3 as NDJSON objects under time-partitioned keys. */
public class S3BatchUploader implements BillingS3LogHandler.AsyncBatchUploader {

  // S3 object-key consistent identifier; TBD
  static final String PATH_PREFIX = "billing-events";
  private static final String NDJSON_CONTENT_TYPE = "application/x-ndjson";
  // object key format
  private static final DateTimeFormatter KEY_TIME_FORMAT =
      DateTimeFormatter.ofPattern("yyyy/MM/dd/HH/mm").withZone(ZoneOffset.UTC);

  // Bound every PUT so a hung connection can neither pin an upload slot indefinitely nor stall
  // the shutdown drain. Retries stay inside the SDK's built-in default policy (bounded attempts,
  // jittered throttle-aware backoff).
  private static final Duration API_CALL_ATTEMPT_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration API_CALL_TIMEOUT = Duration.ofSeconds(30);

  private final S3AsyncClient client;
  private final String bucket;

  S3BatchUploader(S3AsyncClient client, String bucket) {
    this.client = client;
    this.bucket = bucket;
  }

  public static S3BatchUploader create(
      String region, String bucket, Optional<String> endpointOverride) {
    if (region == null || region.isBlank())
      throw new IllegalArgumentException("stargate.jsonapi.billing.s3.bucket-region must be set");
    if (bucket == null || bucket.isBlank())
      throw new IllegalArgumentException("stargate.jsonapi.billing.s3.bucket must be set");
    Objects.requireNonNull(endpointOverride, "endpointOverride must not be null");

    // Credentials resolve from the SDK's default provider chain (env vars, web-identity/OIDC
    // token, instance/container roles), left implicit so the client owns — and closes — the
    // provider. This transparently supports federated (AssumeRoleWithWebIdentity) and
    // cross-account access: the bucket may live in a different account (per IAM + bucket
    // policy); its region is set via .region().
    var builder =
        S3AsyncClient.builder()
            .region(Region.of(region))
            .overrideConfiguration(
                o ->
                    o.apiCallAttemptTimeout(API_CALL_ATTEMPT_TIMEOUT)
                        .apiCallTimeout(API_CALL_TIMEOUT));

    // Real AWS S3 needs no endpoint: the SDK endpoint rules (s3 SDK's DefaultS3EndpointProvider)
    // derive https://<bucket>.s3.<region>.amazonaws.com from region + partition dnsSuffix.
    // An override is only for a non-AWS S3 (S3Mock in tests): it bypasses those rules and forces
    // path-style, since a localhost host can't virtual-host the bucket as a subdomain.
    endpointOverride
        .filter(s -> !s.isBlank())
        .ifPresent(uri -> builder.endpointOverride(URI.create(uri)).forcePathStyle(true));

    return new S3BatchUploader(builder.build(), bucket);
  }

  @Override
  public Uni<Void> upload(BillingQueue.Batch batch) {
    String key = objectKey(batch.oldestEventAt(), UUID.randomUUID());
    byte[] body = toNdjson(batch.lines());
    // No .retry() here: unconfigured, S3AsyncClient already retries (default LegacyRetryStrategy —
    // 3 retries / 4 attempts). See
    // https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/retry-strategy.html
    // and see https://github.com/aws/aws-sdk-java-v2/issues/6987 for future change.
    return Uni.createFrom()
        .completionStage(
            () ->
                client
                    .putObject(
                        PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .contentType(NDJSON_CONTENT_TYPE)
                            .build(),
                        AsyncRequestBody.fromBytes(body))
                    .thenAccept(resp -> {}));
  }

  @VisibleForTesting
  static String objectKey(Instant timestamp, UUID id) {
    return PATH_PREFIX + "/" + KEY_TIME_FORMAT.format(timestamp) + "/" + id + ".jsonl";
  }

  @VisibleForTesting
  static byte[] toNdjson(List<String> lines) {
    StringBuilder sb = new StringBuilder();
    for (String line : lines) {
      sb.append(line).append('\n');
    }
    return sb.toString().getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void close() {
    client.close();
  }
}
