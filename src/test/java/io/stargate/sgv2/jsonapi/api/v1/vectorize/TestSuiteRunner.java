package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TestSuite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.restassured.RestAssured.given;

public class TestSuiteRunner extends RunnerBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestSuiteRunner.class);


  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final TestPlan testPlan;
  private final TestSuite testSuite;
  private final TestEnvironment testEnvironment;

  public TestSuiteRunner(
      TestPlan testPlan, TestSuite testSuite, TestEnvironment testEnvironment) {
    this.testPlan = testPlan;
    this.testSuite = testSuite;
    this.testEnvironment = testEnvironment;
  }



  @Override
  public void execute() throws Throwable {



//    for (TestCase testCase : testSuite.tests()) {
//      var testRequest = new TestRequest(testCase.command(), testPlan.target(), testEnvironment, TestAssertion.buildAssertions(testCase));
//      var testResponse = testRequest.execute();
//      var testCaseResult = testResponse.validate(testSuite, testCase);
//
//      if (testCaseResult.failed()){
//        LOGGER.warn("TestCase FAILED: test.name={}, testCase.name={}, failedAssertion={}, error={}",
//            testCaseResult.integrationTest().meta().name(),testCaseResult.testCase().name(), testCaseResult.failedAssertion(), String.valueOf(testCaseResult.error()));
//      }
//      else{
//        LOGGER.info("TestCase PASSED: test.name={}, testCase.name={}",
//            testCaseResult.integrationTest().meta().name(),testCaseResult.testCase().name());
//      }
//    }

  }

}
