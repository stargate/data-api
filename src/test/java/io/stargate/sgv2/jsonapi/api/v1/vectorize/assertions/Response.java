package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;
import static org.hamcrest.Matchers.hasSize;

public class Response {

  public static TestAssertion isFindSuccess(JsonNode args) {
    return new BodyAssertion("$", responseIsFindSuccess());
  }

  public static TestAssertion isFindAndSuccess(JsonNode args) {
    return new BodyAssertion("$", responseIsFindAndSuccess());
  }


  public static TestAssertion isWriteSuccess(JsonNode args) {
    return new BodyAssertion("$", responseIsWriteSuccess());
  }

  public static TestAssertion isDDLSuccess(JsonNode args) {
    return new BodyAssertion("$", responseIsDDLSuccess());
  }

}
