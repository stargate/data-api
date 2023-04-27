package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.config.HttpClientConfig;
import io.restassured.config.RestAssuredConfig;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.io.InputStream;
import java.util.Collections;
import org.apache.commons.lang3.RandomUtils;
import org.apache.http.params.CoreProtocolPNames;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class HTTPLimitsIntegrationTest extends AbstractNamespaceIntegrationTestBase {

  @Test
  @SuppressWarnings("deprecation") // CoreProtocolPNames deprecated
  public void tryToSendTooBigInsert() {
    // try sending 1.01 MB
    RandomBytesInputStream inputStream = new RandomBytesInputStream(1024 * 1024 + 1024);

    // Fails before getting to business logic no need for real collection (or keyspace fwtw)
    // While documents don't say it, Quarkus tests show 413 as expected fail message:
    given()
        // https://stackoverflow.com/questions/66299813/apache-httpclient-4-5-13-java-net-socketexception-broken-pipe-write-failed
        .config(
            RestAssuredConfig.config()
                .httpClient(
                    HttpClientConfig.httpClientConfig()
                        .addParams(
                            Collections.singletonMap(
                                CoreProtocolPNames.USE_EXPECT_CONTINUE, true))))
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
        .contentType(ContentType.JSON)
        .body(inputStream)
        .when()
        .post(CollectionResource.BASE_PATH, namespaceName, "noSuchCollection")
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
    public int read() {
      if (count++ >= size) {
        return -1;
      }

      return RandomUtils.nextInt(0, 255);
    }
  }
}
