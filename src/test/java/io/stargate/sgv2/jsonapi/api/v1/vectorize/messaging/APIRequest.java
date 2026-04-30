package io.stargate.sgv2.jsonapi.api.v1.vectorize.messaging;

import static io.restassured.RestAssured.given;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import io.stargate.sgv2.jsonapi.api.model.command.CommandTarget;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.targets.Connection;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestRunEnv;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TestCommand;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class APIRequest {
  private static final Logger LOGGER = LoggerFactory.getLogger(APIRequest.class);

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static String COLLECTION_PATH = "/{keyspace}/{collection}";
  private static String KEYSPACE_PATH = "/{keyspace}";
  private static String DB_PATH = "/";

  private final Connection connection;
  private final TestRunEnv integrationEnv;
  private final ObjectNode request;

  public APIRequest(Connection connection, TestRunEnv integrationEnv, ObjectNode request) {

    this.connection = connection;
    this.integrationEnv = integrationEnv;
    this.request = request;
  }

  public APIResponse execute() {

    boolean retry = true;
    int maxAttempts = 3;
    int attempt = 1;
    ValidatableResponse lastValidatableResponse = null;

    while (retry && attempt <=maxAttempts){
      attempt++;
      retry = false;

      // Create a new request spec, there is some state that is left in it when a request is run
      // for path params
      var requestSpec = requestSpec();
      var rawResponse = executeRequest(requestSpec);
      lastValidatableResponse = rawResponse.then();

      // logg even if we retry
      lastValidatableResponse.log().status().and().log().body();

      if (attempt < maxAttempts) {
        var body = rawResponse.body().asString();
        // TODO: XXXX: put this in the test plan
        var retryMatch = List.of("EMBEDDING_PROVIDER_RATE_LIMITED", "EMBEDDING_PROVIDER_TIMEOUT");

        for (var match : retryMatch) {
          if (body.contains(match)) {
            retry = true;
            // Exponential backoff with jitter: 2^attempt seconds base (1s, 2s, 4s...) plus up to 500ms
            // random jitter to avoid thundering herd if multiple tests hit the rate limit simultaneously
            long sleepMs = (long) (1000 * Math.pow(2, attempt)) + ThreadLocalRandom.current().nextLong(500);
            LOGGER.info("executeRequest() - Retrying, found retry string in response. match={}, sleepMs={} ms, attemptCount={}", match, sleepMs, attempt);
            try {
              Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException("Interrupted during retry sleep", e);
            }
          }
        }
      }
    }
    return new APIResponse(this, lastValidatableResponse);

  }

  private RequestSpecification requestSpec() {

    String requestString;
    try {
      requestString = OBJECT_MAPPER.writeValueAsString(request);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    return jsonRequest().body(requestString).when();
  }

  private Response executeRequest(RequestSpecification requestSpec) {

    var commandName = TestCommand.commandName(request);
    Response rawRresponse;
    if (commandName.getTargets().contains(CommandTarget.COLLECTION)
        || commandName.getTargets().contains(CommandTarget.TABLE)) {
      rawRresponse =
          requestSpec.post(
              COLLECTION_PATH,
              integrationEnv.requiredValue("KEYSPACE_NAME"),
              integrationEnv.requiredValue("COLLECTION_NAME"));
    } else if (commandName.getTargets().contains(CommandTarget.KEYSPACE)) {
      rawRresponse = requestSpec.post(KEYSPACE_PATH, integrationEnv.requiredValue("KEYSPACE_NAME"));
    } else if (commandName.getTargets().contains(CommandTarget.DATABASE)) {
      rawRresponse = requestSpec.post(DB_PATH);
    } else {
      throw new IllegalArgumentException("Do not know how to execute command: " + commandName);
    }

    return rawRresponse;
  }

  protected Map<String, String> getHeaders() {

    var headers = new HashMap<String, String>();
    headers.put(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, integrationEnv.requiredValue(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME));

    var embeddingApiKey = integrationEnv.get(HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME);
    if (!Strings.isNullOrEmpty(embeddingApiKey)){
      headers.put(HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME, embeddingApiKey);
    }
    return headers;
  }

  public RequestSpecification jsonRequest() {

    return given()
        .log()
        .uri()
        .log()
        .body()
        .baseUri(connection.domain())
        .port(connection.port())
        .basePath(connection.basePath())
        .headers(getHeaders())
        .contentType(ContentType.JSON);
  }
}
