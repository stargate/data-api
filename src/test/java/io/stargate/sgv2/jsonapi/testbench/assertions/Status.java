package io.stargate.sgv2.jsonapi.testbench.assertions;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.testbench.testspec.TestCommand;

/** Assertions that match Data API status top level response field. */
public class Status {

  static {
    AssertionFactory.REGISTRY.register(Status.class);
  }

  /** Assertion factory, see {@link AssertionFactory.AssertionMatcherFactory} */
  public static AssertionMatcher isExactly(TestCommand testCommand, JsonNode args) {
    return new BodyAssertion("status", jsonEquals(args));
  }
}
