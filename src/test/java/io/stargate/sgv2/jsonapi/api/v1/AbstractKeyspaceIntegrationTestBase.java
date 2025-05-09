package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsDDLSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.util.IntegrationTestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.fail;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.core.Base64Variants;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import io.stargate.sgv2.jsonapi.api.v1.util.IntegrationTestUtils;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.service.embedding.operation.test.CustomITEmbeddingProvider;
import io.stargate.sgv2.jsonapi.testresource.StargateTestResource;
import io.stargate.sgv2.jsonapi.util.Base64Util;
import io.stargate.sgv2.jsonapi.util.CqlVectorUtil;
import java.net.InetSocketAddress;
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
 * Abstract class for all int tests that needs a keyspace to execute tests in. This class
 * automatically creates a keyspace before all tests and drops a keyspace after all test have been
 * run.
 *
 * <p>Note that this test uses a small workaround in {@link #getTestPort()} to avoid issue that
 * Quarkus is not setting-up the rest assured target port in the @BeforeAll and @AfterAll methods
 * (see https://github.com/quarkusio/quarkus/issues/7690).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractKeyspaceIntegrationTestBase {

  // keyspace automatically created in this test
  protected static final String keyspaceName = "ks" + RandomStringUtils.randomAlphanumeric(16);

  @BeforeAll
  public static void enableLog() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @BeforeAll
  public void createKeyspace() {
    createKeyspace(keyspaceName);
  }

  protected void createKeyspace(String nsToCreate) {
    String json =
            """
        {
          "createKeyspace": {
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
        .body("$", responseIsDDLSuccess())
        .body("status.ok", is(1));
  }

  @AfterAll
  public void dropKeyspace() {
    String json =
            """
        {
          "dropKeyspace": {
            "name": "%s"
          }
        }
        """
            .formatted(keyspaceName);

    given()
        .port(getTestPort())
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(GeneralResource.BASE_PATH)
        .then()
        .statusCode(200)
        .body("$", responseIsDDLSuccess())
        .body("status.ok", is(1))
        .body("errors", is(nullValue()));
  }

  protected void createCollection(String keyspace, String collectionToCreate) {
    given()
        .port(getTestPort())
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(
                """
                  {
                    "createCollection": {
                      "name": "%s"
                    }
                  }
                  """
                .formatted(collectionToCreate))
        .when()
        .post(KeyspaceResource.BASE_PATH, keyspace)
        .then()
        .statusCode(200)
        .body("$", responseIsDDLSuccess());
  }

  protected void deleteCollection(String collectionName) {
    given()
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(
                """
                    {
                      "deleteCollection": {
                        "name": "%s"
                      }
                    }
                    """
                .formatted(collectionName))
        .when()
        .post(KeyspaceResource.BASE_PATH, keyspaceName)
        .then()
        .statusCode(200)
        .body("$", responseIsDDLSuccess())
        .body("status.ok", is(1));
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

  public static void checkShouldAbsentMetrics(String commandName) {
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
    assertThat(countMetrics.size()).isEqualTo(0);
  }

  public static void checkDriverMetricsTenantId() {
    String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
    // Example line
    // session_cql_requests_seconds{module="sgv2-jsonapi",session="default_tenant",quantile="0.5",}
    // 0.238944256

    Optional<String> sessionLevelDriverMetricTenantId =
        metrics
            .lines()
            .filter(
                line ->
                    line.startsWith("session_cql_requests_seconds") && line.contains("session="))
            .findFirst();

    List<String> lines = metrics.lines().toList();
    if (!sessionLevelDriverMetricTenantId.isPresent()) {
      long buckets =
          lines.stream()
              .filter(line -> line.startsWith("session_cql_requests_seconds_bucket"))
              .count();
      fail(
          String.format(
              "No tenant id found in 'session_cql_requests_seconds_bucket' (%d buckets; %d log lines)",
              buckets, lines.size()));
    }
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

  public static void checkIndexUsageMetrics(String commandName, boolean vector) {
    String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
    List<String> countMetrics =
        metrics
            .lines()
            .filter(
                line -> {
                  if (vector)
                    return line.startsWith("index_usage_count")
                        && line.contains("command=\"" + commandName + "\"")
                        && line.contains("query_vector_value=\"true\"");
                  else
                    return line.startsWith("index_usage_count")
                        && line.contains("command=\"" + commandName + "\"")
                        && line.contains("query_vector_value=\"false\"");
                })
            .toList();
    assertThat(countMetrics.size()).isGreaterThan(0);
  }

  /** Utility method for reducing boilerplate code for sending JSON commands */
  protected RequestSpecification givenHeadersAndJson(String json) {
    return given().headers(getHeaders()).contentType(ContentType.JSON).body(json);
  }

  protected String generateBase64EncodedBinaryVector(float[] vector) {
    {
      final byte[] byteArray = CqlVectorUtil.floatsToBytes(vector);

      // Encode the byte array into a Base64 string
      return Base64Variants.MIME_NO_LINEFEEDS.encode(byteArray);
    }
  }

  protected float[] decodeBase64BinaryVectorToFloatArray(String base64Vector) {
    byte[] binaryPayload;
    try {
      binaryPayload = Base64Util.decodeFromMimeBase64(base64Vector);
      return CqlVectorUtil.bytesToFloats(binaryPayload);
    } catch (Exception e) {
      throw new RuntimeException("Not a valid binary vector", e);
    }
  }

  protected boolean executeCqlStatement(SimpleStatement... statements) {
    var cqlSession = createDriverSession();
    for (SimpleStatement statement : statements) {
      if (!cqlSession.execute(statement).wasApplied()) {
        cqlSession.close();
        return false;
      }
    }
    cqlSession.close();
    return true;
  }

  private CqlSession createDriverSession() {
    int port =
        useCoordinator()
            ? Integer.getInteger(IntegrationTestUtils.STARGATE_CQL_PORT_PROP)
            : Integer.getInteger(IntegrationTestUtils.CASSANDRA_CQL_PORT_PROP);
    String dc;
    if (StargateTestResource.isDse() || StargateTestResource.isHcd()) {
      dc = "dc1";
    } else {
      dc = "datacenter1";
    }
    var builder =
        new CqlSessionBuilder()
            .withLocalDatacenter(dc)
            .addContactPoint(new InetSocketAddress("localhost", port))
            .withAuthCredentials("cassandra", "cassandra"); // default admin password :)
    return builder.build();
  }

  /** Helper method for determining if lexical search is available for the database backend */
  protected boolean isLexicalAvailableForDB() {
    return !"true".equals(System.getProperty("testing.db.lexical-disabled"));
  }

  /** Utility method for reducing boilerplate code for sending JSON commands */
  protected ValidatableResponse givenHeadersPostJsonThen(String json) {
    return givenHeadersAndJson(json).when().post(KeyspaceResource.BASE_PATH, keyspaceName).then();
  }

  /** Utility method for reducing boilerplate code for sending JSON commands */
  protected ValidatableResponse givenHeadersPostJsonThenOk(String json) {
    return givenHeadersPostJsonThen(json).statusCode(200);
  }

  /** Utility method for reducing boilerplate code for sending JSON commands */
  protected ValidatableResponse givenHeadersPostJsonThenOkNoErrors(String json) {
    return givenHeadersPostJsonThenOk(json).body("errors", is(nullValue()));
  }
}
