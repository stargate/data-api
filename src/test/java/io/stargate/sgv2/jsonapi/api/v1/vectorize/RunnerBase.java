package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;

import java.util.Map;

import static io.restassured.RestAssured.given;

public abstract class RunnerBase {

  protected abstract IntegrationEnv  integrationEnv();

  protected Map<String, String> getHeaders() {

    var env = integrationEnv();
    return Map.of(
        HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
        env.requiredValue("Token"),
        HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
        env.requiredValue("x-embedding-api-key"));
  }

  public RequestSpecification jsonRequest(){

    return given()
        .log().all()
        .port(8181)
        .headers(getHeaders())
        .contentType(ContentType.JSON);

  }
}
