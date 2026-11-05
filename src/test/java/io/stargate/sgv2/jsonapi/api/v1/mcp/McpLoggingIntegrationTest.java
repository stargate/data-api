package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import io.vertx.core.MultiMap;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/** Exercises log redaction through real driver callbacks in the packaged application. */
@QuarkusIntegrationTest
@WithTestResource(DseTestResource.class)
@TestProfile(McpLoggingIntegrationTest.LoggingProfile.class)
class McpLoggingIntegrationTest extends McpIntegrationTestBase {
  private static final Path LOG_FILE =
      Path.of("target/mcp-packaged-logging-test.log").toAbsolutePath();
  private static final String PRIVATE_FIELD = "synthetic_private_index_field";

  @Override
  protected MultiMap authHeaders() {
    return super.authHeaders().add("User-Agent", "synthetic_private_user_agent");
  }

  @Test
  void collectionMetadataStaysRedactedAcrossDriverCallbacks() throws Exception {
    createKeyspace(keyspaceName);
    try {
      long start = Files.size(LOG_FILE);
      createCollection(
          keyspaceName,
          collectionName,
          Map.of("indexing", Map.of("allow", List.of(PRIVATE_FIELD))));

      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () -> {
                byte[] bytes = Files.readAllBytes(LOG_FILE);
                String logs =
                    new String(
                        Arrays.copyOfRange(bytes, Math.toIntExact(start), bytes.length),
                        StandardCharsets.UTF_8);
                var requestId =
                    Pattern.compile(
                            "MCP command completed requestId=([a-f0-9-]+) command=createCollection isError=false")
                        .matcher(logs);
                assertThat(requestId.find()).as("The command has a generated request ID").isTrue();
                assertThat(logs.contains("MCP request completed requestId=" + requestId.group(1)))
                    .as("Command and HTTP completion share the request ID")
                    .isTrue();
                assertThat(
                        logs.lines()
                            .anyMatch(
                                line ->
                                    line.contains("CreateCollectionOperation")
                                        && line.contains("MCP event details redacted")))
                    .as("Collection metadata logging is active and redacted")
                    .isTrue();
                assertThat(logs.contains(PRIVATE_FIELD))
                    .as("Private collection metadata stays out of logs")
                    .isFalse();
                assertThat(logs.contains("synthetic_private_user_agent"))
                    .as("Request cache metadata stays out of logs")
                    .isFalse();
                assertThat(logs.contains("requestId=null")).isFalse();
              });
    } finally {
      dropKeyspace(keyspaceName);
    }
  }

  public static class LoggingProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "quarkus.log.file.enabled", "true",
          "quarkus.log.file.path", LOG_FILE.toString(),
          "quarkus.log.category.\"io.stargate.sgv2.jsonapi.service.operation.collections.CreateCollectionOperation\".level",
              "DEBUG");
    }
  }
}
