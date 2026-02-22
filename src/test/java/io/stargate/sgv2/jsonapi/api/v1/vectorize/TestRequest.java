package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.BodyAssertion;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.TestAssertion;

import java.util.List;

public record TestRequest(TestCommand testCommand,
                          IntegrationTarget integrationTarget,
                          IntegrationEnv integrationEnv,
                          List<TestAssertion> testAssertions) {

  public TestResponse execute(){

    var apiRequest = integrationTarget.apiRequest(testCommand, integrationEnv);
    return new TestResponse(this, apiRequest, apiRequest.execute());
  }
}
