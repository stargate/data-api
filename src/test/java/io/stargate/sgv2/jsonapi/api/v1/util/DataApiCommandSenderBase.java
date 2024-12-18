package io.stargate.sgv2.jsonapi.api.v1.util;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.util.IntegrationTestUtils.getAuthToken;
import static io.stargate.sgv2.jsonapi.api.v1.util.IntegrationTestUtils.getCassandraPassword;
import static io.stargate.sgv2.jsonapi.api.v1.util.IntegrationTestUtils.getCassandraUsername;

import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.service.embedding.operation.test.CustomITEmbeddingProvider;
import jakarta.ws.rs.core.Response;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Helper class used for constructing and sending commands to the Data API for testing purposes
 * (usually integration tests).
 */
public abstract class DataApiCommandSenderBase<T extends DataApiCommandSenderBase> {
  protected final String keyspace;

  protected Response.Status expectedHttpStatus = Response.Status.OK;

  protected final Map<String, String> headers;

  protected DataApiCommandSenderBase(String keyspace) {
    this.keyspace = keyspace;
    headers = DefaultHeaders.mutableCopy();
  }

  /**
   * Fluent method for setting the expected HTTP status code of the response; default is 200.
   *
   * @param expectedHttpStatus Status to expect
   * @return Type-safe "this" sender for call chaining
   */
  public T expectHttpStatus(Response.Status expectedHttpStatus) {
    this.expectedHttpStatus = expectedHttpStatus;
    return _this();
  }

  /**
   * Fluent method for adding/overriding/removing a header in the request.
   *
   * @param name Name of header to set
   * @param value Value of header to set; if null, header is removed, otherwise added or overridden
   * @return Type-safe "this" sender for call chaining
   */
  public T header(String name, String value) {
    if (value == null) {
      headers.remove(name);
    } else {
      headers.put(name, value);
    }
    return _this();
  }

  protected T _this() {
    return (T) this;
  }

  /**
   * "Untyped" method for sending a POST command to the Data API: caller is responsible for
   * formatting the JSON body correctly.
   *
   * @param jsonBody JSON body to POST
   * @return Response validator for further assertions
   */
  protected final DataApiResponseValidator postJSON(CommandName commandName, String jsonBody) {
    RequestSpecification request =
        given()
            .port(getTestPort())
            .headers(headers)
            .contentType(ContentType.JSON)
            .body(jsonBody)
            .when();
    ValidatableResponse response =
        postInternal(request).then().statusCode(expectedHttpStatus.getStatusCode());
    return new DataApiResponseValidator(commandName, response);
  }

  /**
   * <b>NOTE:</b> please use the commands on the subclasses if possible, they are more specific this
   * is public to make it iterate sending commands, if there is a missing function on the subclass
   * add it.
   */
  public DataApiResponseValidator postCommand(CommandName commandName, String commandClause) {
    return postJSON(
        commandName, "{ \"%s\": %s }".formatted(commandName.getApiName(), commandClause));
  }

  protected abstract io.restassured.response.Response postInternal(RequestSpecification request);

  protected static int getTestPort() {
    try {
      return ConfigProvider.getConfig().getValue("quarkus.http.test-port", Integer.class);
    } catch (Exception e) {
      return Integer.parseInt(System.getProperty("quarkus.http.test-port"));
    }
  }

  private static class DefaultHeaders {
    private static final Map<String, String> HEADERS = collectDefaultHeaders();

    public static Map<String, String> mutableCopy() {
      return new LinkedHashMap<>(HEADERS);
    }

    private static Map<String, String> collectDefaultHeaders() {
      final boolean useCoordinator = Boolean.getBoolean("testing.containers.use-coordinator");
      if (useCoordinator) {
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
  }
}
