package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import io.stargate.sgv2.jsonapi.api.model.command.CommandTarget;
import io.stargate.sgv2.jsonapi.api.v1.CollectionResource;
import io.stargate.sgv2.jsonapi.api.v1.KeyspaceResource;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;

import java.util.Map;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsDDLSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsWriteSuccess;
import static org.hamcrest.Matchers.*;


public class APIRequest {

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static String COLLECTION_PATH  = "/{keyspace}/{collection}";
  private static String KEYSPACE_PATH  = "/{keyspace}";
  private static String DB_PATH  = "/";

  private final Connection connection;
  private final TestRequest testRequest;
  private final IntegrationEnv env;

  public APIRequest( Connection connection, TestRequest testRequest, IntegrationEnv  env ) {
    this.connection = connection;
    this.testRequest = testRequest;
    this.env = env;
  }

  public ValidatableResponse execute(){

    var requestWithEnv = testRequest.withEnvironment(env);
    var requestSpec = requestSpec(requestWithEnv);
    return executeRequest(requestSpec);
  }

  public ValidatableResponse executeWithSuccess(){
    var resp = execute();
    assertSuccess(resp);
    return resp;
  }

  private RequestSpecification requestSpec(ObjectNode request) {

    String requestString;
    try {
      requestString = OBJECT_MAPPER.writeValueAsString(request);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    return jsonRequest()
        .body(requestString).when();
  }

  private ValidatableResponse executeRequest(RequestSpecification requestSpec) {

    var commandName = testRequest.commandName();
    Response response;
    if (commandName.getTargets().contains(CommandTarget.COLLECTION) || commandName.getTargets().contains(CommandTarget.TABLE)){
      response = requestSpec
          .post(COLLECTION_PATH, env.requiredValue("KEYSPACE_NAME"), env.requiredValue("COLLECTION_NAME"));
    }
    else if (commandName.getTargets().contains(CommandTarget.KEYSPACE) ){
      response =  requestSpec.post(KEYSPACE_PATH, env.requiredValue("KEYSPACE_NAME"));
    }
    else if(commandName.getTargets().contains(CommandTarget.DATABASE)){
      response =  requestSpec.post(DB_PATH);
    }
    else {
      throw new IllegalArgumentException("Do not know how to execute command: " + testRequest.commandName());
    }

    return response
        .then().log().all();
  }

  protected Map<String, String> getHeaders() {

    return Map.of(
        HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
        env.requiredValue("Token"),
        HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
        env.requiredValue("x-embedding-api-key"));
  }

  public RequestSpecification jsonRequest(){

    return given()
        .log().all()
        .baseUri(connection.domain())
        .port(connection.port())
        .basePath(connection.basePath())
        .headers(getHeaders())
        .contentType(ContentType.JSON);

  }

  private void assertSuccess(ValidatableResponse response) {
    response.statusCode(200);

    switch (testRequest.commandName()) {
      case INSERT_ONE, INSERT_MANY -> {
        response
            .body("$", responseIsWriteSuccess())
            .body("status.insertedIds[0]", not(emptyString()));
      }
      case DELETE_COLLECTION, CREATE_COLLECTION -> {
        response
            .body("$", responseIsDDLSuccess())
            .body("status.ok", is(1));
      }
    }
  }
}
