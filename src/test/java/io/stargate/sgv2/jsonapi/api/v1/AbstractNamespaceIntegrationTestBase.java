package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.util.IntegrationTestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.service.embedding.operation.test.CustomITEmbeddingProvider;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    createNamespace(namespaceName);
  }

  protected void createNamespace(String nsToCreate) {
    String json =
            """
        {
          "createNamespace": {
            "name": "%s"
          }
        }
        """
            .formatted(nsToCreate);

    given()
        .port(getTestPort())
        .headers(getHeaders())
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
        .headers(getHeaders())
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

  protected Map<String, ?> getHeaders() {
    if (useCoordinator()) {
      return Map.of(
          HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
          getAuthToken(),
          HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
          CustomITEmbeddingProvider.TEST_API_KEY);
    } else {
      String credential =
          "Cassandra:"
              + Base64.getEncoder().encodeToString(getCassandraUsername().getBytes())
              + ":"
              + Base64.getEncoder().encodeToString(getCassandraPassword().getBytes());
      return Map.of(
          HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
          credential,
          HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
          CustomITEmbeddingProvider.TEST_API_KEY);
    }
  }

  protected Map<String, ?> getInvalidHeaders() {
    if (useCoordinator()) {
      return Map.of(
          HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
          "invalid",
          HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
          CustomITEmbeddingProvider.TEST_API_KEY);
    } else {
      String credential =
          "Cassandra:"
              + Base64.getEncoder().encodeToString("invalid".getBytes())
              + ":"
              + Base64.getEncoder().encodeToString(getCassandraPassword().getBytes());
      return Map.of(
          HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
          credential,
          HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
          CustomITEmbeddingProvider.TEST_API_KEY);
    }
  }

  protected boolean useCoordinator() {
    return Boolean.getBoolean("testing.containers.use-coordinator");
  }

  public static void checkMetrics(String commandName) {
    String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
    List<String> countMetrics =
        metrics
            .lines()
            .filter(
                line ->
                    line.startsWith("command_processor_process")
                        && line.contains("command=\"" + commandName + "\"")
                        && line.contains("vector_enabled=\"false\""))
            .toList();
    assertThat(countMetrics.size()).isGreaterThan(0);
  }

  public static void checkDriverMetricsTenantId() {
    String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
    Optional<String> sessionLevelDriverMetricTenantId =
        metrics
            .lines()
            .filter(
                line ->
                    line.startsWith("session_cql_requests_seconds_bucket")
                        && line.contains("tenant"))
            .findFirst();
    assertThat(sessionLevelDriverMetricTenantId.isPresent()).isTrue();
  }

  public static void checkVectorMetrics(String commandName, String sortType) {
    String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
    List<String> countMetrics =
        metrics
            .lines()
            .filter(
                line ->
                    line.startsWith("command_processor_process")
                        && line.contains("command=\"" + commandName + "\"")
                        && line.contains("vector_enabled=\"true\"")
                        && line.contains("sort_type=\"" + sortType + "\""))
            .toList();
    assertThat(countMetrics.size()).isGreaterThan(0);
  }
}
