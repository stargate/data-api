package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import org.apache.http.HttpStatus;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;

public class Statuscode {

  public static AssertionMatcher success(JsonNode args){
    return apiResponse -> apiResponse.validatableResponse().statusCode(HttpStatus.SC_OK);
  }
}
