package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.BodyAssertion;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.TestAssertion;

public record TestResponse(
    TestRequest testRequest,
    APIRequest apiRequest,
    APIResponse apiResponse
) {

  public TestCaseResult validate(IntegrationTest integrationTest, TestCase testCase){
    return validate(integrationTest, testCase, false);
  }

  public TestCaseResult validate(IntegrationTest integrationTest, TestCase testCase, boolean throwOnError) {

    AssertionError assertionError = null;
    TestAssertion failedAssertion = null;
    for (var testAssertion : testRequest.testAssertions()){
      try{
        testAssertion.run(apiResponse);
      }
      catch(AssertionError e){
        if (throwOnError){
          throw e;
        }

        failedAssertion = testAssertion;
        assertionError = e;
        break;
      }
    }
    return new TestCaseResult(integrationTest, testCase, this  , assertionError, failedAssertion);
  }
}
