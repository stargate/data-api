package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsDDLSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsWriteSuccess;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import io.stargate.sgv2.jsonapi.testresource.S3MockTestResource;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * End-to-end test of the billing S3 export: real vectorize commands (via {@code
 * CustomITEmbeddingProvider}) emit {@code billing.events} lines, and the installed {@code
 * BillingS3LogHandler} must land them in the S3Mock bucket as time-partitioned NDJSON objects.
 *
 * <p>{@link S3MockTestResource} enables the export with small thresholds (count seal 5, age sweep
 * 2s) and turns on the {@code billing-events-logging} feature flag.
 */
@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@WithTestResource(value = S3MockTestResource.class)
public class BillingS3ExportIntegrationTest extends AbstractKeyspaceIntegrationTestBase {

  private static final String COLLECTION = "billing_export_collection";

  /** Every vectorize call emits at least one billing event, so lines >= documents. */
  private static final int DOCUMENTS = 10;

  private static final Pattern KEY_PATTERN =
      Pattern.compile("billing-events/\\d{4}/\\d{2}/\\d{2}/\\d{2}/\\d{2}/[0-9a-f-]{36}\\.jsonl");

  /** Wire contract of {@code BillingEventType}: billing consumers key on these exact values. */
  private static final Set<String> EVENT_TYPES =
      Set.of(
          "internal_model_total_tokens",
          "external_model_total_tokens",
          "internal_model_egress_bytes",
          "external_model_egress_bytes",
          "internal_model_ingress_bytes",
          "external_model_ingress_bytes");

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void billingEventsLandInS3AsNdjson() throws Exception {
    createVectorizeCollection();
    for (int i = 0; i < DOCUMENTS; i++) {
      insertDocumentWithVectorize(i);
    }

    try (S3Client s3 = verificationClient()) {
      // The count seal ships full batches immediately; the 2s age tick sweeps the remainder.
      await()
          .atMost(Duration.ofSeconds(60))
          .pollInterval(Duration.ofSeconds(2))
          .untilAsserted(
              () -> assertThat(exportedLines(s3)).hasSizeGreaterThanOrEqualTo(DOCUMENTS));

      // Object layout: time-partitioned keys and NDJSON content type.
      List<S3Object> objects = exportObjects(s3);
      assertThat(objects).isNotEmpty();
      for (S3Object object : objects) {
        assertThat(object.key()).matches(KEY_PATTERN);
      }
      var head = s3.headObject(b -> b.bucket(S3MockTestResource.BUCKET).key(objects.get(0).key()));
      assertThat(head.contentType()).isEqualTo("application/x-ndjson");

      // Every line is a self-contained billing event with the expected shape; ids never repeat
      // across objects. (region/resource_id may be absent locally and are not asserted.)
      List<String> lines = exportedLines(s3);
      Set<String> seenIds = new HashSet<>();
      for (String line : lines) {
        JsonNode event = MAPPER.readTree(line);
        String id = event.path("id").asText();
        assertThat(id).isNotBlank();
        assertThat(seenIds.add(id))
            .as("billing event id duplicated across export: %s", id)
            .isTrue();
        assertThat(event.path("timestamp").asText()).isNotBlank();
        assertThat(event.path("product").asText()).isEqualTo("serverless");
        assertThat(event.path("event_type").asText()).isIn(EVENT_TYPES);
        JsonNode properties = event.path("properties");
        assertThat(properties.path("usage").isIntegralNumber()).isTrue();
        assertThat(properties.path("usage").asLong()).isGreaterThanOrEqualTo(0L);
        assertThat(properties.path("resource_type").asText()).isEqualTo("serverless_database");
        assertThat(properties.path("provider").asText()).isEqualTo("custom");
        // The billed model is what the provider reports in ModelUsage — for the IT provider that
        // is its internal model config ("test-model"), not the createCollection modelName.
        assertThat(properties.path("model").asText()).isEqualTo("test-model");
      }
    }

    // The delivery counters on /metrics must agree that the export is alive.
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              String metrics =
                  given().when().get("/metrics").then().statusCode(200).extract().asString();
              double flushedTotal =
                  metrics
                      .lines()
                      .filter(line -> line.startsWith("billing_s3_events_flushed_total"))
                      .mapToDouble(
                          line -> Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1)))
                      .sum();
              assertThat(flushedTotal).isGreaterThanOrEqualTo(DOCUMENTS);
            });
  }

  // ============================================================
  // Command helpers
  // ============================================================

  private void createVectorizeCollection() {
    givenHeadersPostJsonThenOk(
                """
            {
                "createCollection": {
                    "name": "%s",
                    "options": {
                        "vector": {
                            "metric": "cosine",
                            "dimension": 5,
                            "service": {
                                "provider": "custom",
                                "modelName": "text-embedding-ada-002",
                                "authentication": {
                                    "providerKey" : "shared_creds.providerKey"
                                },
                                "parameters": {
                                    "projectId": "test project"
                                }
                            }
                        }
                    }
                }
            }
            """
                .formatted(COLLECTION))
        .body("$", responseIsDDLSuccess())
        .body("status.ok", is(1));
  }

  private void insertDocumentWithVectorize(int i) {
    String json =
            """
        {
           "insertOne": {
              "document": {
                  "_id": "doc-%d",
                  "description": "billing export test document %d",
                  "$vectorize": "billing export test document %d"
              }
           }
        }
        """
            .formatted(i, i, i);
    givenHeadersAndJson(json)
        .when()
        .post(CollectionResource.BASE_PATH, keyspaceName, COLLECTION)
        .then()
        .statusCode(200)
        .body("$", responseIsWriteSuccess());
  }

  // ============================================================
  // S3 verification helpers
  // ============================================================

  private static S3Client verificationClient() {
    return S3Client.builder()
        .region(Region.of(S3MockTestResource.BUCKET_REGION))
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(
                    S3MockTestResource.ACCESS_KEY, S3MockTestResource.SECRET_KEY)))
        .endpointOverride(URI.create(S3MockTestResource.endpoint()))
        .forcePathStyle(true)
        .build();
  }

  private static List<S3Object> exportObjects(S3Client s3) {
    return s3.listObjectsV2(b -> b.bucket(S3MockTestResource.BUCKET).prefix("billing-events/"))
        .contents();
  }

  private static List<String> exportedLines(S3Client s3) {
    List<String> lines = new ArrayList<>();
    for (S3Object object : exportObjects(s3)) {
      String body =
          s3.getObjectAsBytes(b -> b.bucket(S3MockTestResource.BUCKET).key(object.key()))
              .asUtf8String();
      body.lines().filter(line -> !line.isBlank()).forEach(lines::add);
    }
    return lines;
  }
}
