package io.stargate.sgv2.jsonapi.service.provider;

import java.net.URI;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Production {@link BillingS3LogHandler.AsyncBatchUploader} backed by an AWS SDK v2 {@link
 * S3AsyncClient}: each sealed batch is one async {@code PutObject}, returned as a {@link
 * CompletionStage} so the handler's pipeline drives it without blocking. The async client's Netty
 * HTTP backend is already on the classpath (pulled by {@code bedrockruntime}), so this needs no new
 * sync HTTP-client dependency.
 *
 * <p>Credentials come from the {@link DefaultCredentialsProvider} chain (IRSA web-identity token in
 * AWS deployments). When an {@code endpointOverride} is configured (e.g. S3Mock in tests),
 * path-style addressing is forced so bucket-as-host resolution does not get in the way.
 */
public class S3BatchUploader implements BillingS3LogHandler.AsyncBatchUploader {

  private static final String NDJSON_CONTENT_TYPE = "application/x-ndjson";

  private final S3AsyncClient client;
  private final String bucket;

  S3BatchUploader(S3AsyncClient client, String bucket) {
    this.client = client;
    this.bucket = bucket;
  }

  /**
   * Builds an uploader from resolved config. {@code region} and {@code bucket} must be non-null.
   */
  public static S3BatchUploader create(
      String region, String bucket, Optional<String> endpointOverride) {
    Objects.requireNonNull(region, "region must not be null");
    Objects.requireNonNull(bucket, "bucket must not be null");

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

    return new S3BatchUploader(builder.build(), bucket);
  }

  @Override
  public CompletionStage<Void> upload(String key, byte[] body) {
    // Returns the async PUT future (a failed future drives the handler's retry/backoff); no
    // blocking.
    return client
        .putObject(
            PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType(NDJSON_CONTENT_TYPE)
                .build(),
            AsyncRequestBody.fromBytes(body))
        .thenAccept(resp -> {});
  }

  @Override
  public void close() {
    client.close();
  }
}
