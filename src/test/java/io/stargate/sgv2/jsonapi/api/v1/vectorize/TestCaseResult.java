package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.TestAssertion;

public record TestCaseResult(
    TestSuite integrationTest,
    TestCase testCase, // Nullable
    TestResponse testResponse,
    AssertionError error,
    TestAssertion failedAssertion
) {

  public boolean failed(){
    return error != null;
  }
}
