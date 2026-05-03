package io.stargate.sgv2.jsonapi.testbench.assertions;

import static io.stargate.sgv2.jsonapi.testbench.assertions.DescribableAssertionMatcher.described;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.testbench.testspec.TestCommand;
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
