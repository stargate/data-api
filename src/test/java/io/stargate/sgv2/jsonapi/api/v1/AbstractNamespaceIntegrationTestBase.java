package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

/**
 * Abstract class for all int tests that needs a namespace to execute tests in. This class
 * automatically creates a namespace before all tests and drops a namespace after all test have been
 * run.
 *
 * <p>Note that this test uses a small workaround in {@link #getTestPort()} to avoid issue that
 * Quarkus is not setting-up the rest assured target port in the @BeforeAll and @AfterAll methods
 * (see https://github.com/quarkusio/quarkus/issues/7690).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractNamespaceIntegrationTestBase {

  // namespace automatically created in this test
  protected final String namespaceName = "ns" + RandomStringUtils.randomAlphanumeric(16);

  @BeforeAll
  public static void enableLog() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @BeforeAll
  public void createNamespace() {
    String json =
        """
        {
          "createNamespace": {
            "name": "%s"
          }
        }
        """
            .formatted(namespaceName);

    given()
        .port(getTestPort())
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(GeneralResource.BASE_PATH)
        .then()
        .statusCode(200)
        .body("status.ok", is(1))
        .body("errors", is(nullValue()));
  }

  @AfterAll
  public void dropNamespace() {
    String json =
        """
        {
          "dropNamespace": {
            "name": "%s"
          }
        }
        """
            .formatted(namespaceName);

    given()
        .port(getTestPort())
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(GeneralResource.BASE_PATH)
        .then()
        .statusCode(200)
        .body("status.ok", is(1))
        .body("errors", is(nullValue()));
  }

  protected int getTestPort() {
    try {
      return ConfigProvider.getConfig().getValue("quarkus.http.test-port", Integer.class);
    } catch (Exception e) {
      return Integer.parseInt(System.getProperty("quarkus.http.test-port"));
    }
  }

  public void checkMetrics(String commandName) {
    String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
    List<String> countMetrics =
        metrics
            .lines()
            .filter(
                line ->
                    line.startsWith("command_processor_count")
                        && line.contains("command=\"" + commandName + "\""))
            .toList();
    assertThat(countMetrics.size()).isGreaterThan(0);

    List<String> totalMetrics =
        metrics
            .lines()
            .filter(
                line ->
                    line.startsWith("command_processor_total")
                        && line.contains("command=\"" + commandName + "\""))
            .toList();
    assertThat(totalMetrics.size()).isGreaterThan(0);
  }
}
