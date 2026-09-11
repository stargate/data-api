package io.stargate.sgv2.jsonapi.service.billing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.smallrye.config.SmallRyeConfigBuilder;
import io.stargate.sgv2.jsonapi.config.BillingS3ExportConfig;
import io.stargate.sgv2.jsonapi.metrics.BatchedLogUploaderMetrics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

/**
 * The S3 client is mocked; retries and per-call timeouts live in the client configuration, so
 * exactly one {@code putObject} per upload is expected here. Real I/O is covered by {@code
 * BillingS3ExportIntegrationTest}.
 */
class S3BatchedLogUploaderTest {

  private static final Instant OLDEST_EVENT_AT = Instant.parse("2026-05-20T16:23:00Z");
  private static final String LINE_A = "{\"a\":1}"; // him
  private static final String LINE_B = "{\"b\":2}";
  private static final int LINE_BYTE_SIZE = 16; // 7 chars per line and one for new line
  private static final BatchedLogBuffer.Batch BATCH =
      new BatchedLogBuffer.Batch(
          BatchedLogBuffer.BillingBatchReason.DRAINING,
          List.of(LINE_A, LINE_B),
          LINE_BYTE_SIZE, // 7 chars per line and one for new line
          OLDEST_EVENT_AT);
  private static final String EXPECTED_KEY =
      "data-api/2026/05/20/16/23/%s.jsonl".formatted(BATCH.id());

  @Test
  void createReadsConfig() {

    var fixture = Fixture.create(true);

    var toString = fixture.uploader.toString();

    assertThat(toString)
        .as("toString() contains region)")
        .contains("region=" + fixture.config.region());
    assertThat(toString)
        .as("toString() contains bucket)")
        .contains("bucket=" + fixture.config.bucket());
    assertThat(toString)
        .as("toString() contains pathPrefix)")
        .contains("pathPrefix=" + fixture.config.s3PathPrefix());

    // TODO: test for endpoint override.
  }

  @Test
  void closeCascadedToS3Client() {

    var fixture = Fixture.create();

    fixture.uploader.close();
    verify(fixture.s3Client, times(1)).close();
  }

  /** ObjectKey is buikld using the properties of the Batch */
  @Test
  void objectLocationUsesBatchProperties() {

    var fixture = Fixture.create();

    var location = fixture.uploader.objectLocation(BATCH);
    assertThat(location.region()).as("region is same as config").isEqualTo(fixture.config.region());

    assertThat(location.bucket()).as("bucket is same as config").isEqualTo(fixture.config.bucket());

    assertThat(location.key()).as("key is as expected").isEqualTo(EXPECTED_KEY);
  }

  /** Object body built using properties of the Batch */
  @Test
  void objectContentContainsBatchLines() {

    var fixture = Fixture.create();

    var bytes = fixture.uploader.objectContent(BATCH);

    assertThat(bytes)
        .as("bytes is as expected")
        .isEqualTo((LINE_A + "\n" + LINE_B + "\n").getBytes(StandardCharsets.UTF_8));
  }

  /** Request to S3 clent matches the batch and config properties */
  @Test
  void uploadUsesCorrectBatchProperties() {
    var fixture = Fixture.create();

    fixture.uploader.upload(BATCH);

    var requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
    var bodyCaptor = ArgumentCaptor.forClass(AsyncRequestBody.class);
    verify(fixture.s3Client, times(1)).putObject(requestCaptor.capture(), bodyCaptor.capture());

    var request = requestCaptor.getValue();
    assertThat(request.bucket()).as("bucket is same as config").isEqualTo(fixture.config.bucket());
    assertThat(request.key()).as("key is as expected").isEqualTo(EXPECTED_KEY);
    assertThat(request.contentType())
        .as("content type is as expected")
        .isEqualTo(S3BatchedLogUploader.CONTENT_TYPE_NDJSON);

    var body = bodyCaptor.getValue();
    assertThat(body.contentLength()).as("content length matches batch").hasValue(BATCH.bytes());
  }

  /** Multiple calls to upload generate different keys, each matching batch properties */
  @Test
  void uploadGeneratesUniqueKeys() {
    var fixture = Fixture.create();

    // second batch we will send, has diff Id and 1 hour later.
    var oldestEvent2 = OLDEST_EVENT_AT.plus(1, ChronoUnit.HOURS);
    var batch2 =
        new BatchedLogBuffer.Batch(
            BatchedLogBuffer.BillingBatchReason.MAX_BYTES_EXCEEDED,
            BATCH.lines(),
            LINE_BYTE_SIZE,
            oldestEvent2);

    fixture.uploader.upload(BATCH);
    fixture.uploader.upload(batch2);

    var requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
    var bodyCaptor = ArgumentCaptor.forClass(AsyncRequestBody.class);
    verify(fixture.s3Client, times(2)).putObject(requestCaptor.capture(), bodyCaptor.capture());

    var requests = requestCaptor.getAllValues();

    assertThat(requests.stream().map(PutObjectRequest::key))
        .as("all put requests keys are unique")
        .doesNotHaveDuplicates();

    assertThat(requests.getFirst().key()).as("key for batch 1 as expected").isEqualTo(EXPECTED_KEY);

    var expectedKey2 = "data-api/2026/05/20/17/23/%s.jsonl".formatted(batch2.id());
    assertThat(requests.get(1).key()).as("key for batch 2 as expected").isEqualTo(expectedKey2);
  }

  @Test
  void uploadS3FailureReturned() {

    var fixture = Fixture.create();
    var expectedThrowable = new RuntimeException("S3 Failed");
    when(fixture.s3Client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(CompletableFuture.failedFuture(expectedThrowable));

    var uploadResult = fixture.uploader.upload(BATCH).await().atMost(Duration.ofSeconds(10));

    verify(
            fixture.s3Client,
            times(1)
                .description("s3Client upload() called once, all retry is internal to s3 client"))
        .putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));

    assertThat(uploadResult).isNotNull();
    assertThat(uploadResult.throwable())
        .as("uploadResult has expected throwable instance")
        .isSameAs(expectedThrowable);
    assertThat(uploadResult.batch()).as("uploadResult has expected batch instance").isSameAs(BATCH);
  }

  @Test
  void uploadS3SuccessReturned() {

    var fixture = Fixture.create();

    var uploadResult = fixture.uploader.upload(BATCH).await().atMost(Duration.ofSeconds(10));

    verify(
            fixture.s3Client,
            times(1)
                .description("s3Client upload() called once, all retry is internal to s3 client"))
        .putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));

    assertThat(uploadResult).isNotNull();
    assertThat(uploadResult.throwable()).as("uploadResult throwable is null when success").isNull();
    assertThat(uploadResult.batch()).as("uploadResult has expected batch instance").isSameAs(BATCH);
  }

  // ============================================================
  // Scaffold
  // ============================================================

  private record Fixture(
      S3BatchedLogUploader uploader,
      S3AsyncClient s3Client,
      PutObjectResponse putResponse,
      BatchedLogUploaderMetrics metrics,
      BillingS3ExportConfig config) {

    static Fixture create() {
      return create(false);
    }

    static Fixture create(boolean useFactory) {

      var config =
          new SmallRyeConfigBuilder()
              .withMapping(BillingS3ExportConfig.class)
              .build()
              .getConfigMapping(BillingS3ExportConfig.class);

      var metrics = mock(BatchedLogUploaderMetrics.class);

      var s3Client = mock(S3AsyncClient.class);

      var sdkResponse = SdkHttpResponse.builder().statusCode(200).build();
      var builder = PutObjectResponse.builder();
      // sdkHttpResponse is on an inherited builder, return type changes
      builder.sdkHttpResponse(sdkResponse);
      var putResponse = builder.build();

      when(s3Client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
          .thenReturn(CompletableFuture.completedFuture(putResponse));

      S3BatchedLogUploader uploader;
      if (useFactory) {
        uploader =
            S3BatchedLogUploader.create(
                config.region(),
                config.bucket(),
                config.endpointOverride().orElse(null),
                config.s3PathPrefix(),
                config.s3CallAttemptTimeout(),
                config.s3TotalCallTimeout(),
                config.s3RetryMode(),
                metrics);
      } else {
        uploader =
            new S3BatchedLogUploader(
                s3Client, config.region(), config.bucket(), config.s3PathPrefix(), metrics);
      }

      return new Fixture(uploader, s3Client, putResponse, metrics, config);
    }
  }
}
