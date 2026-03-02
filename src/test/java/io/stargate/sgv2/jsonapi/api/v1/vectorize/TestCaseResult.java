package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.AssertionMatcher;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TestSuite;

public record TestCaseResult(
    TestSuite integrationTest,
    TestCase testCase, // Nullable
    TestResponse testResponse,
    AssertionError error,
    AssertionMatcher failedAssertion
) {

  public boolean failed(){
    return error != null;
  }
}
