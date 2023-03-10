package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.common.CqlEnabledIntegrationTestBase;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junitpioneer.jupiter.RetryingTest;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class HTTPLimitsIntegrationTest extends CqlEnabledIntegrationTestBase {
  private final ObjectMapper objectMapper = new JsonMapper();

  // For some reason there are failures esp by Native Image, for "Broken Pipe": RestAssured
  // client having trouble getting connection closed before sending whole payload.
  // But seems to be transient issue, related to timing. So retry a few times
  @RetryingTest(5)
  public void tryToSendTooBigInsert() {
    // Need to generate payload above 1 meg: need NOT be valid wrt Document
    // constraints (i.e. can have like 10k array elements) because request
    // size must be validated before Document constraints.
    ObjectNode root = objectMapper.createObjectNode();
    root.put("_id", "too-big-1");
    ArrayNode dataArray = root.putArray("data");

    // ~100k numbers with 9 digits (plus separators etc) goes above 1 meg
    for (int row = 0; row < 1024; ++row) {
      ArrayNode rowArray = dataArray.addArray();
      // 9 digits plus separating comma (plus [ and ]) make it about this:
      for (int chars = 0; chars < 1024; chars += 10) {
        rowArray.add(123456789);
      }
    }

    String json =
        """
                {
                  "insertOne": {
                    "document": %s
                  }
                }
            """
            .formatted(root.toString());
    // Sanity check payload is between 1 and 2 megs
    assertThat(json.length()).isGreaterThan(1 * 1024 * 1024);
    assertThat(json.length()).isLessThan(2 * 1024 * 1024);

    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        // Fails before getting to business logic no need for real collection (or keyspace fwtw)
        .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), "noSuchCollection")
        .then()
        // While docs don't say it, Quarkus tests show 413 as expected fail message:
        .statusCode(413);
  }
}
