package io.stargate.sgv2.jsonapi.testbench.messaging;

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
import io.stargate.sgv2.jsonapi.testbench.targets.ConnectionConfiguration;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestRunEnv;
import io.stargate.sgv2.jsonapi.testbench.testspec.TestCommand;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * The lowest level to represent a request sent to the API, that is only concerned with the
 * mechanics of sending the request.
 * <p>
 * Handles retries based on detecting substrings in the response body, these occur before
 * {@link #execute()} returns.
 * </p>
 */
public class APIRequest {
  private static final Logger LOGGER = LoggerFactory.getLogger(APIRequest.class);

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static int RETRY_MAX_ATTEMPTS = 5;

  // API paths based on the target of the command.
  private static String COLLECTION_TABLE_PATH = "/{keyspace}/{collection}";
  private static String KEYSPACE_PATH = "/{keyspace}";
  private static String DB_PATH = "/";

  private final ConnectionConfiguration connection;
  private final TestRunEnv testRunEnv;
  private final ObjectNode request;
  private final APIRetryPolicy retryPolicy;

  /**
   * Initializes a new instance of the class.
   * @param connection Connection info for the API instance to use.
   * @param testRunEnv Environment for this test run, used to find schema names to use in the URL path.
   * @param request the complete API request to send, <b>NOTE:</b> any substitutions into the body of the
   *                request must also be done.
   */
  public APIRequest(ConnectionConfiguration connection, TestRunEnv testRunEnv, ObjectNode request) {

    this.connection = connection;
    this.testRunEnv = testRunEnv;
    this.request = request;
    this.retryPolicy = APIRetryPolicy.createRetryPolicy(testRunEnv);
  }

  /**
   * Executes the request, including any retries.
   * <p>
   * No validation of the response is performed, that is left for assertions to handle later.
   * </p>
   * @return {@link APIResponse} holding the response of the request.
   */
  public APIResponse execute() {

    ValidatableResponse lastValidatableResponse = null;

    var retryDecision = retryPolicy.firstAttempt();
    while (retryDecision.retry()){

      // Create a new request spec, there is some state that is left in it when a request is run
      // for path params
      var requestSpec = requestSpec();
      var rawResponse = executeRequest(requestSpec);
      lastValidatableResponse = rawResponse.then();

      // log even if we retry, the request will be logged and it makes sense to see the respose that
      // caused the retry
      lastValidatableResponse.log().status().and().log().body();
      retryDecision = retryPolicy.decide(retryDecision, rawResponse);
    }
    return new APIResponse(this, lastValidatableResponse);
  }

  /**
   * Get the request ready to send our request.
   * @return
   */
  private RequestSpecification requestSpec() {

    String requestString;
    try {
      requestString = OBJECT_MAPPER.writeValueAsString(request);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    return requestForTaget()
            .headers(getHeaders())
            .body(requestString).when();
  }

  /**
   * Create a new RequestSpecification for JSON to send to the target
   */
  private RequestSpecification requestForTaget() {

    return given()
            .log()
            .uri()
            .log()
            .body()
            .baseUri(connection.domain())
            .port(connection.port())
            .basePath(connection.basePath())
            .contentType(ContentType.JSON);
  }

  /**
   * Get the wellknown headers we need to send with the request.
   */
  protected Map<String, String> getHeaders() {

    var headers = new HashMap<String, String>();
    headers.put(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, testRunEnv.requiredValue(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME));

    var embeddingApiKey = testRunEnv.get(HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME);
    if (!Strings.isNullOrEmpty(embeddingApiKey)){
      headers.put(HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME, embeddingApiKey);
    }
    return headers;
  }

  private Response executeRequest(RequestSpecification requestSpec) {

    var commandName = TestCommand.commandName(request);
    Response rawRresponse;

    // URL to send to depends on the target of the command.
    if (commandName.getTargets().contains(CommandTarget.COLLECTION)
        || commandName.getTargets().contains(CommandTarget.TABLE)) {
      rawRresponse =
          requestSpec.post(
                  COLLECTION_TABLE_PATH,
              testRunEnv.requiredValue(TestRunEnv.ENV_KEYSPACE_NAME),
              testRunEnv.requiredValue(TestRunEnv.ENV_COLLECTION_NAME));
    } else if (commandName.getTargets().contains(CommandTarget.KEYSPACE)) {
      rawRresponse = requestSpec.post(KEYSPACE_PATH, testRunEnv.requiredValue(TestRunEnv.ENV_KEYSPACE_NAME));
    } else if (commandName.getTargets().contains(CommandTarget.DATABASE)) {
      rawRresponse = requestSpec.post(DB_PATH);
    } else {
      throw new IllegalArgumentException("Do not know how to execute command: " + commandName);
    }

    return rawRresponse;
  }

}
