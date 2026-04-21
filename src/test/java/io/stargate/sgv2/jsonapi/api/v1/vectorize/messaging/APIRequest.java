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

import java.util.HashMap;
import java.util.Map;

public class APIRequest {

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

    var requestSpec = requestSpec();
    return new APIResponse(this, executeRequest(requestSpec));
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

  private ValidatableResponse executeRequest(RequestSpecification requestSpec) {

    var commandName = TestCommand.commandName(request);
    Response response;
    if (commandName.getTargets().contains(CommandTarget.COLLECTION)
        || commandName.getTargets().contains(CommandTarget.TABLE)) {
      response =
          requestSpec.post(
              COLLECTION_PATH,
              integrationEnv.requiredValue("KEYSPACE_NAME"),
              integrationEnv.requiredValue("COLLECTION_NAME"));
    } else if (commandName.getTargets().contains(CommandTarget.KEYSPACE)) {
      response = requestSpec.post(KEYSPACE_PATH, integrationEnv.requiredValue("KEYSPACE_NAME"));
    } else if (commandName.getTargets().contains(CommandTarget.DATABASE)) {
      response = requestSpec.post(DB_PATH);
    } else {
      throw new IllegalArgumentException("Do not know how to execute command: " + commandName);
    }

    return response.then().log().all();
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
