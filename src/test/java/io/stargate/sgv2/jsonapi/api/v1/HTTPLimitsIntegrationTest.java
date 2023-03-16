package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.config.HttpClientConfig;
import io.restassured.config.RestAssuredConfig;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.common.CqlEnabledIntegrationTestBase;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.io.IOException;
import java.io.InputStream;
import org.apache.http.client.config.*;
import org.apache.http.impl.client.*;
import org.jetbrains.annotations.NotNull;
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
        .config(
            RestAssuredConfig.config()
                .httpClient(
                    HttpClientConfig.httpClientConfig()
                        .httpClientFactory(customHttpClientFactory())))
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
        .contentType(ContentType.JSON)
        .body(inputStream)
        .when()
        .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), "noSuchCollection")
        .then()
        .statusCode(413);
  }

  // enables setExpectContinueEnabled to true
  // from
  // https://stackoverflow.com/questions/66299813/apache-httpclient-4-5-13-java-net-socketexception-broken-pipe-write-failed
  @NotNull
  private static HttpClientConfig.HttpClientFactory customHttpClientFactory() {
    return () -> {
      RequestConfig requestConfig = RequestConfig.custom().setExpectContinueEnabled(true).build();
      return HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
    };
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
