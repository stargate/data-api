package io.stargate.sgv2.jsonapi.testbench.assertions;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.testbench.testspec.TestCommand;

public class Status {

  static {
    AssertionFactory.REGISTRY.register(Status.class);
  }

  public static AssertionMatcher isExactly(TestCommand testCommand, JsonNode args) {
    return new BodyAssertion("status", jsonEquals(args));
  }
}
