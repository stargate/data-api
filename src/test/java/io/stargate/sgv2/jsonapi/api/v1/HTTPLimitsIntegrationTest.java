package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.common.CqlEnabledIntegrationTestBase;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.io.IOException;
import java.io.InputStream;
import org.junit.jupiter.api.RepeatedTest;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomUtils;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class HTTPLimitsIntegrationTest extends CqlEnabledIntegrationTestBase {

  @RepeatedTest(5)
  public void tryToSendTooBigInsert() {
    // try sending 1.01 MB
    RandomBytesInputStream inputStream = new RandomBytesInputStream(1024 * 1024 + 1024);

    // Fails before getting to business logic no need for real collection (or keyspace fwtw)
    // While docs don't say it, Quarkus tests show 413 as expected fail message:
    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
        .contentType(ContentType.JSON)
        .body(inputStream)
        .when()
        .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), "noSuchCollection")
        .then()
        .statusCode(413);
  }

  static class RandomBytesInputStream extends InputStream {

    private final int size;

    private int count;

    RandomBytesInputStream(int size) {
      this.size = size;
    }

    @Override
    public int read() throws IOException {
      if (count++ >= size) {
        return -1;
      }

      return RandomUtils.nextInt(0, 255);
    }
  }
}
