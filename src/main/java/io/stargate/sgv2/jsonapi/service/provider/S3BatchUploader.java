package io.stargate.sgv2.jsonapi.service.provider;

import io.smallrye.mutiny.Uni;
import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * {@link BillingS3LogHandler.AsyncBatchUploader} backed by an AWS SDK v2 {@link S3AsyncClient}:
 * each sealed batch is one async {@code PutObject} with bounded retry/backoff, returned as a {@link
 * Uni} so the handler's pipeline drives it without blocking.
 *
 * <p>Credentials come from the {@link DefaultCredentialsProvider} chain (IRSA web-identity token in
 * AWS deployments). When an {@code endpointOverride} is configured (e.g. S3Mock in tests),
 * path-style addressing is forced so bucket-as-host resolution does not get in the way.
 */
public class S3BatchUploader implements BillingS3LogHandler.AsyncBatchUploader {

  private static final String NDJSON_CONTENT_TYPE = "application/x-ndjson";

  private final S3AsyncClient client;
  private final String bucket;
  private final RetryPolicy retry;

  S3BatchUploader(S3AsyncClient client, String bucket, RetryPolicy retry) {
    this.client = client;
    this.bucket = bucket;
    this.retry = retry;
  }

  /**
   * Builds an uploader from resolved inputs. {@code endpointOverride} is present only for a non-AWS
   * S3 (e.g. S3Mock in tests).
   */
  public static S3BatchUploader create(
      String region, String bucket, Optional<String> endpointOverride, RetryPolicy retry) {
    if (region == null || region.isBlank())
      throw new IllegalArgumentException("stargate.jsonapi.billing.s3.bucket-region must be set");
    if (bucket == null || bucket.isBlank())
      throw new IllegalArgumentException("stargate.jsonapi.billing.s3.bucket must be set");
    Objects.requireNonNull(endpointOverride, "endpointOverride must not be null");
    Objects.requireNonNull(retry, "retry must not be null");

    var builder =
        S3AsyncClient.builder()
            .region(Region.of(region))
            // Credentials resolve from the SDK's default provider chain (env vars,
            // web-identity/OIDC token, instance/container roles). This transparently supports
            // federated (AssumeRoleWithWebIdentity) and cross-account access — the bucket may live
            // in a different account (per IAM + bucket policy); its region is set via .region().
            .credentialsProvider(DefaultCredentialsProvider.create());

    // Real AWS S3 needs no endpoint: the SDK endpoint rules (s3 SDK's DefaultS3EndpointProvider)
    // derive https://<bucket>.s3.<region>.amazonaws.com from region + partition dnsSuffix.
    // An override is only for a non-AWS S3 (S3Mock in tests): it bypasses those rules and forces
    // path-style, since a localhost host can't virtual-host the bucket as a subdomain.
    endpointOverride
        .filter(s -> !s.isBlank())
        .ifPresent(uri -> builder.endpointOverride(URI.create(uri)).forcePathStyle(true));

    return new S3BatchUploader(builder.build(), bucket, retry);
  }

  @Override
  public Uni<Void> upload(String key, byte[] body) {
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
                    .thenAccept(resp -> {}))
        .onFailure()
        .retry()
        .withBackOff(retry.initialBackOff(), retry.maxBackOff())
        .withJitter(retry.jitter())
        .atMost(retry.atMostRetries());
  }

  @Override
  public void close() {
    client.close();
  }

  /**
   * Bounded exponential-backoff-with-jitter tuning for one PUT's retries. Validated on construction
   * so bad tuning fails at wiring time with a clear message.
   */
  public record RetryPolicy(
      int atMostRetries, Duration initialBackOff, Duration maxBackOff, double jitter) {
    public RetryPolicy {
      if (atMostRetries < 0) {
        throw new IllegalArgumentException("atMostRetries must be >= 0");
      }
      if (initialBackOff.isNegative() || initialBackOff.isZero()) {
        throw new IllegalArgumentException("initialBackOff must be > 0");
      }
      if (maxBackOff.compareTo(initialBackOff) < 0) {
        throw new IllegalArgumentException("maxBackOff must be >= initialBackOff");
      }
      if (jitter < 0 || jitter > 1) {
        throw new IllegalArgumentException("jitter must be in [0, 1]");
      }
    }
  }
}
