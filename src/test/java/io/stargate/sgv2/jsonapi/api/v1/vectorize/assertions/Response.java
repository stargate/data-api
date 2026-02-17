package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsFindSuccess;
import static org.hamcrest.Matchers.hasSize;

public class Response {

  public static ITAssertion isFindSuccess(JsonNode args) {
    return new ITAssertion("$", responseIsFindSuccess());
  }
}
