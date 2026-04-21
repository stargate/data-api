package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import static io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.DescribableAssertionMatcher.described;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TestCommand;
import org.apache.http.HttpStatus;

public class Http {

  static {
    AssertionFactory.REGISTRY.register(Http.class);
  }

  public static AssertionMatcher success(TestCommand testCommand, JsonNode args) {
    return described(
        "http status is " + HttpStatus.SC_OK,
        apiResponse -> apiResponse.validatable().statusCode(HttpStatus.SC_OK));
  }
}
