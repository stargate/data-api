package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.APIResponse;
import org.apache.http.HttpStatus;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;

public class StatusCode {

  public static TestAssertion success(JsonNode args){
    return apiResponse -> apiResponse.validatableResponse().statusCode(HttpStatus.SC_OK);
  }
}
