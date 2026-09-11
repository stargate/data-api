package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.*;
import static io.stargate.sgv2.jsonapi.util.MetricsITAssertions.assertMetricTotal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.service.billing.BillingEventType;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import io.stargate.sgv2.jsonapi.testresource.S3MockTestResource;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Integration tests for billing that use a mock S3 backend to confirm events are sent to S3 that
 * look OK. And that S3 being down does not fail Data API Commands.
 *
 * <p><b>NOTE:</b> we first made billing for findAndRerank / rerank models, these tests ony use
 * $vectorize with insert, not findAndRerank, because we have test infra for a custom vectorizer
 *
 * <p>TestMethodOrder is used because the second test turns off the S3 container so we need that to
 * be last
 */
@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@WithTestResource(value = S3MockTestResource.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class) // needed because we turn off mock S3
public class BillingS3ExportIntegrationTest extends AbstractCollectionIntegrationTestBase {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(BillingS3ExportIntegrationTest.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final TestConstants TEST_CONSTANTS = new TestConstants();

  private static final int DOCUMENT_COUNT = 10;
  private static final Pattern KEY_PATTERN =
      Pattern.compile("data-api/\\d{4}/\\d{2}/\\d{2}/\\d{2}/\\d{2}/[0-9a-f-]{36}\\.jsonl");
  private static final Set<String> EVENT_TYPES =
      new HashSet<>(BillingEventType.ALL.stream().map(BillingEventType::eventName).toList());

  @BeforeAll
  public void setup() {
    assertDatabaseCommand()
        .templated()
        .createKeyspace(TEST_CONSTANTS.KEYSPACE_NAME)
        .wasSuccessful();

    createCollection();
  }

  @AfterAll
  public void tearDown() {
    assertDatabaseCommand().templated().dropKeyspace(TEST_CONSTANTS.KEYSPACE_NAME).wasSuccessful();
  }

  /**
   * Send
   *
   * @throws Exception
   */
  @Test
  public void billingEventsSentToS3() throws Exception {

    try (var s3Client = s3ClientForVerification()) {

      insertDocs("billingEventsSentToS3()");

      // need to wait for billing to send events to S3.
      // there should be at least 1 billing event per document we sent
      await()
          .atMost(Duration.ofSeconds(60))
          .pollInterval(Duration.ofSeconds(2))
          .untilAsserted(
              () ->
                  assertThat(allLinesInAllObjectsInBucket(s3Client))
                      .hasSizeGreaterThanOrEqualTo(DOCUMENT_COUNT));

      var allS3Objects = allObjectsInBucket(s3Client);

      // Do all the keys match the expected pattern ?
      assertThat(allS3Objects).isNotEmpty();
      for (var s3Object : allS3Objects) {
        assertThat(s3Object.key())
            .as("billingEventsSentToS3() - object key matches expected pattern")
            .matches(KEY_PATTERN);
      }

      // get the metadata for the first Object we got back
      // here "HEAD" means get the headers, leave the body.
      // Kind of like "Leave the gun take the cannoli" but less shooty
      var head =
          s3Client.headObject(
              b -> b.bucket(S3MockTestResource.BUCKET).key(allS3Objects.getFirst().key()));
      assertThat(head.contentType())
          .as("billingEventsSentToS3() - ContentType of first object matches expected")
          .isEqualTo("application/x-ndjson");

      // Validate the billing events we have are valid JSON and do some simple property checks
      // not checking the values make sense, just the ones we expect to be set and a few things.
      // this is not checking that the event is "well-formed"
      var allBillingLines = allLinesInAllObjectsInBucket(s3Client);
      Set<String> seenIds = new HashSet<>();

      // JSON Pointers
      var requiredTextMembers =
          List.of(
              "/id",
              "/timestamp",
              "/product",
              "/event_type",
              "/properties/resource_type",
              "/properties/provider",
              "/properties/model");
      var requiredNumericMembers = List.of("/properties/usage");

      for (String line : allBillingLines) {
        var event = MAPPER.readTree(line);

        assertIsSet(JsonNodeType.STRING, event, requiredTextMembers);
        assertIsSet(JsonNodeType.NUMBER, event, requiredNumericMembers);

        assertThat(seenIds.add(event.path("id").asText()))
            .as("billing event id duplicated across export: " + event.path("id").asText())
            .isTrue();
        assertThat(event.path("product").asText())
            .as("event product is expected")
            .isEqualTo("serverless");
        assertThat(event.path("event_type").asText())
            .as("event type is from expected set: " + EVENT_TYPES)
            .isIn(EVENT_TYPES);

        assertThat(event.at("/properties/usage").asLong())
            .as("event properties.usage is > 0")
            .isGreaterThanOrEqualTo(0L);
        assertThat(event.at("/properties/resource_type").asText())
            .as("event properties.resource_type is expected")
            .isEqualTo("serverless_database");
        assertThat(event.at("/properties/provider").asText())
            .as("event properties.provider is expected from collection creation")
            .isEqualTo("custom");

        // The billed model is what the provider reports in ModelUsage — for the IT provider that
        // is its internal model config ("test-model"), not the createCollection modelName.
        assertThat(event.at("/properties/model").asText()).isEqualTo("test-model");
      }
    } // end of S3 resource

    // Check the metrics reflect that we sent the expected number of events
    assertMetricTotal(
        "billing.s3.uploaded.events", (a) -> a.isGreaterThanOrEqualTo(DOCUMENT_COUNT));
    assertMetricTotal("billing.buffer.offered", (a) -> a.isGreaterThanOrEqualTo(DOCUMENT_COUNT));
    assertMetricTotal("billing.buffer.dropped", (a) -> a.isZero());
  }

  /**
   * Confirm the API still works when the S3 backend is offline.
   *
   * <p><b>NOTE:</b> must run last so because the S3 resource is shared
   */
  @Test
  @Order(Integer.MAX_VALUE)
  public void exportFailureDoesNotAffectTheApi() {
    S3MockTestResource.stopContainer();

    // assert happens in the function, this is the test, could we insert docs ?
    insertDocs("exportFailureDoesNotAffectTheApi() - 1st");

    // wait until we register a failed upload
    assertMetricTotal("billing.s3.failed.batches", (a) -> a.isGreaterThan(0));
    assertMetricTotal("billing.s3.failed.events", (a) -> a.isGreaterThan(0));

    // double check API still works
    insertDocs("exportFailureDoesNotAffectTheApi() - 2nd");
  }

  // ============================================================
  // Scaffold
  // ============================================================

  private void createCollection() {
    var collectionOptions =
        """
            {
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
          """;

    assertNamespaceCommand(TEST_CONSTANTS.KEYSPACE_NAME)
        .templated()
        .createCollection(TEST_CONSTANTS.COLLECTION_NAME, collectionOptions)
        .wasSuccessful();
  }

  private void insertDocs(String idPrefix) {
    for (int i = 0; i < DOCUMENT_COUNT; i++) {
      var id = "doc-%s-%d".formatted(idPrefix, i);
      var doc =
              """
              {
                  "_id": "doc-%s",
                  "description": "billing export test document %s",
                  "$vectorize": "billing export test document %s"
              }"""
              .formatted(id, id, id);
      assertTableCommand(TEST_CONSTANTS.KEYSPACE_NAME, TEST_CONSTANTS.COLLECTION_NAME)
          .templated()
          .insertOne(doc)
          .wasSuccessful();
    }
  }

  private void assertIsSet(JsonNodeType nodeType, JsonNode parent, List<String> memberPointers) {

    for (var pointer : memberPointers) {
      var child = parent.at(pointer);
      assertThat(child.isMissingNode()).as("child member expected pointer:" + pointer).isFalse();
      assertThat(child.getNodeType()).as("node type is expected:" + nodeType).isEqualTo(nodeType);

      switch (nodeType) {
        case STRING -> assertThat(child.asText()).isNotBlank();
        case NUMBER -> assertThat(child.isNumber()).isTrue();
        default -> {
          throw new IllegalStateException("assertIsSet() - Unexpected node type: " + nodeType);
        }
      }
      ;
    }
  }

  private static S3Client s3ClientForVerification() {

    return S3Client.builder()
        .region(Region.of(S3MockTestResource.REGION))
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(
                    S3MockTestResource.ACCESS_KEY, S3MockTestResource.SECRET_KEY)))
        .endpointOverride(URI.create(S3MockTestResource.endpoint()))
        .forcePathStyle(true)
        .build();
  }

  private static List<S3Object> allObjectsInBucket(S3Client s3Client) {
    var objects =
        s3Client
            .listObjectsV2(b -> b.bucket(S3MockTestResource.BUCKET).prefix("data-api/"))
            .contents();
    LOGGER.info("allObjectInBucket() - objects: {}", objects);
    return objects;
  }

  private static List<String> allLinesInAllObjectsInBucket(S3Client s3Client) {

    List<String> lines = new ArrayList<>();
    for (var s3Object : allObjectsInBucket(s3Client)) {

      var request =
          GetObjectRequest.builder().bucket(S3MockTestResource.BUCKET).key(s3Object.key()).build();
      LOGGER.info("allLinesInAllObjectsInBucket() - getting file. request: {}", request);

      var objectBody = s3Client.getObjectAsBytes(request).asUtf8String();
      objectBody.lines().filter(line -> !line.isBlank()).forEach(lines::add);
    }
    LOGGER.info("allLinesInAllObjectsInBucket() - got all lines from S3. lines:{}", lines);
    return lines;
  }
}
