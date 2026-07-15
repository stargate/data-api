package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

/**
 * The S3 client is mocked; retries and per-call timeouts live in the client configuration, so
 * exactly one {@code putObject} per upload is expected here. Real I/O is covered by {@code
 * BillingS3ExportIntegrationTest}.
 */
class S3BatchUploaderTest {

  private static final Duration AWAIT = Duration.ofSeconds(5);
  private static final Pattern KEY_PATTERN =
      Pattern.compile("billing-events/2026/05/20/14/23/[0-9a-f-]{36}\\.jsonl");
  private static final String LINE_A = "{\"a\":1}";
  private static final String LINE_B = "{\"b\":2}";
  private static final BillingQueue.Batch BATCH =
      new BillingQueue.Batch(List.of(LINE_A, LINE_B), Instant.parse("2026-05-20T14:23:11.482Z"));

  private static S3BatchUploader uploader(S3AsyncClient client) {
    return new S3BatchUploader(client, "my-bucket");
  }

  private static CompletableFuture<PutObjectResponse> ok() {
    return CompletableFuture.completedFuture(PutObjectResponse.builder().build());
  }

  // ============================================================
  // object layout — key and body
  // ============================================================

  @Test
  void objectKeyUsesPathPrefixAndUtcMinutePathFromTimestamp() {
    var id = UUID.fromString("8c0e9b8a-1d3a-4f6b-9c0d-1234567890ab");
    var key = S3BatchUploader.objectKey(Instant.parse("2026-05-20T14:23:11.482Z"), id);
    assertThat(key)
        .isEqualTo("billing-events/2026/05/20/14/23/8c0e9b8a-1d3a-4f6b-9c0d-1234567890ab.jsonl");
  }

  @Test
  void toNdjsonJoinsLinesVerbatimWithTrailingNewlines() {
    assertThat(S3BatchUploader.toNdjson(List.of(LINE_A, LINE_B)))
        .isEqualTo((LINE_A + "\n" + LINE_B + "\n").getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void putsTheNdjsonBodyAtATimePartitionedKey() {
    S3AsyncClient client = mock(S3AsyncClient.class);
    when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(ok());

    uploader(client).upload(BATCH).await().atMost(AWAIT);

    var req = ArgumentCaptor.forClass(PutObjectRequest.class);
    var body = ArgumentCaptor.forClass(AsyncRequestBody.class);
    verify(client).putObject(req.capture(), body.capture());
    assertThat(req.getValue().bucket()).isEqualTo("my-bucket");
    // The key's minute path comes from the batch's oldestEventAt, not the wall clock.
    assertThat(req.getValue().key()).matches(KEY_PATTERN.pattern());
    assertThat(req.getValue().contentType()).isEqualTo("application/x-ndjson");
    assertThat(body.getValue().contentLength())
        .hasValue((long) (LINE_A + "\n" + LINE_B + "\n").getBytes(StandardCharsets.UTF_8).length);
  }

  @Test
  void eachUploadGetsAFreshKey() {
    S3AsyncClient client = mock(S3AsyncClient.class);
    when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(ok());

    var uploader = uploader(client);
    uploader.upload(BATCH).await().atMost(AWAIT);
    uploader.upload(BATCH).await().atMost(AWAIT);

    var req = ArgumentCaptor.forClass(PutObjectRequest.class);
    verify(client, times(2)).putObject(req.capture(), any(AsyncRequestBody.class));
    assertThat(req.getAllValues().stream().map(PutObjectRequest::key)).doesNotHaveDuplicates();
  }

  // ============================================================
  // failure — surfaces once; retries belong to the SDK client
  // ============================================================

  @Test
  void uploadFailurePropagatesAndPutsExactlyOnceAtThisLayer() {
    S3AsyncClient client = mock(S3AsyncClient.class);
    when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("simulated S3 failure")));

    var uploader = uploader(client);
    assertThatThrownBy(() -> uploader.upload(BATCH).await().atMost(AWAIT))
        .hasMessageContaining("simulated S3 failure");

    // retries and per-call timeouts live in the client configuration, so exactly one putObject per upload is expected here
    verify(client, times(1)).putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));
  }

  // ============================================================
  // lifecycle + config validation
  // ============================================================

  @Test
  void closeClosesTheClient() {
    S3AsyncClient client = mock(S3AsyncClient.class);
    uploader(client).close();
    verify(client).close();
  }

  @Test
  void createRejectsMissingRegionOrBucket() {
    assertThatThrownBy(() -> S3BatchUploader.create(" ", "bucket", Optional.empty()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("bucket-region");
    assertThatThrownBy(() -> S3BatchUploader.create("us-east-1", null, Optional.empty()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("billing.s3.bucket");
  }
}
