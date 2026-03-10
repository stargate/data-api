package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TestCommand;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;

public class Status {

  static {
    AssertionFactory.REGISTRY.register(Status.class);
  }


  public static AssertionMatcher isExactly(TestCommand testCommand, JsonNode args) {
    return new BodyAssertion("status", jsonEquals(args));
  }
}
