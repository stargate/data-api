package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestCommand;
import org.apache.http.HttpStatus;

public class Http {

  static {
    AssertionMatcher.FACTORY_REGISTRY.register(Http.class);
  }

  public static AssertionMatcher success(TestCommand testCommand, JsonNode args){
    return apiResponse -> apiResponse.validatableResponse().statusCode(HttpStatus.SC_OK);
  }
}
