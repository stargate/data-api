package io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.messaging.APIRequest;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.messaging.APIResponse;

public record TestRunResponse(
    TestRunRequest testRequest,
    APIRequest apiRequest,
    APIResponse apiResponse
) {

//  public TestCaseResult validate(TestSuite integrationTest, TestCase testCase){
//    return validate(integrationTest, testCase, true);
//  }
//
//  public TestCaseResult validate(TestSuite testSuite, TestCase testCase, boolean throwOnError) {
//
//    AssertionError assertionError = null;
//    AssertionMatcher failedAssertion = null;
//    for (var testAssertion : testRequest.testAssertions()){
//      try{
//        testAssertion.run(apiResponse);
//      }
//      catch(AssertionError e){
//        if (throwOnError){
//          throw e;
//        }
//
//        failedAssertion = testAssertion;
//        assertionError = e;
//        break;
//      }
//    }
//    return new TestCaseResult(testSuite, testCase, this  , assertionError, failedAssertion);
//  }
}
