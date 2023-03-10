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
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class HTTPLimitsIntegrationTest extends CollectionResourceBaseIntegrationTest {
  private final ObjectMapper objectMapper = new JsonMapper();

  @Test
  public void tryToSendTooBigInsert() {
    // Need to generate payload above 1 meg: need NOT be valid wrt Document
    // constraints (i.e. can have like 10k array elements) because request
    // size must be validated before Document constraints.
    ObjectNode root = objectMapper.createObjectNode();
    root.put("_id", "too-big-1");
    ArrayNode dataArray = root.putArray("data");

    // 100k numbers with 9 digits (plus separators etc) goes above 1 meg
    for (int row = 0; row < 1000; ++row) {
      ArrayNode rowArray = dataArray.addArray();
      for (int col = 0; col < 100; ++col) {
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
    assertThat(json.length()).isGreaterThan(1_000_000);

    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
        .then()
        // While docs don't say it, Quarkus tests show 413 as expected fail message:
        .statusCode(413);
  }
}
