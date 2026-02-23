package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.TestAssertion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.restassured.RestAssured.given;

public class IntegrationTestRunner extends RunnerBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationTestRunner.class);


  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final TestPlan testPlan;
  private final TestSuite testSuite;
  private final TestEnvironment testEnvironment;

  public IntegrationTestRunner(
      TestPlan testPlan, TestSuite testSuite, TestEnvironment testEnvironment) {
    this.testPlan = testPlan;
    this.testSuite = testSuite;
    this.testEnvironment = testEnvironment;
  }

  @Override
  protected TestEnvironment integrationEnv() {
    return testEnvironment;
  }

  public void run() {

    LOGGER.info("Starting Integration Test with env={}", testEnvironment);
    for (TestCommand setupCommand : testSuite.setup()) {
      var setupRequest = new TestRequest(setupCommand, testPlan.target(), testEnvironment, TestAssertion.forSuccess(setupCommand.commandName()));

      var setupResponse = setupRequest.execute();
      var testCaseResult = setupResponse.validate(testSuite, null);

      if (testCaseResult.failed()){
        LOGGER.warn("TestSetup FAILED: test.name={}, testCase.name={}, failedAssertion={}, error={}",
            testCaseResult.integrationTest().meta().name(), "NULL", testCaseResult.failedAssertion(), String.valueOf(testCaseResult.error()));
      }
      else{
        LOGGER.info("TestSetup PASSED: test.name={}, testCase.name={}",
            testCaseResult.integrationTest().meta().name(), "NULL");
      }

    }

    for (TestCase testCase : testSuite.tests()) {
      var testRequest = new TestRequest(testCase.command(), testPlan.target(), testEnvironment, TestAssertion.buildAssertions(testCase));
      var testResponse = testRequest.execute();
      var testCaseResult = testResponse.validate(testSuite, testCase);

      if (testCaseResult.failed()){
        LOGGER.warn("TestCase FAILED: test.name={}, testCase.name={}, failedAssertion={}, error={}",
            testCaseResult.integrationTest().meta().name(),testCaseResult.testCase().name(), testCaseResult.failedAssertion(), String.valueOf(testCaseResult.error()));
      }
      else{
        LOGGER.info("TestCase PASSED: test.name={}, testCase.name={}",
            testCaseResult.integrationTest().meta().name(),testCaseResult.testCase().name());
      }
    }

    for (TestCommand cleanupCommand : testSuite.cleanup()) {

      var cleanupRequest = new TestRequest(cleanupCommand, testPlan.target(), testEnvironment, TestAssertion.forSuccess(cleanupCommand.commandName()));

      var cleanupResponse = cleanupRequest.execute();
      var testCaseResult = cleanupResponse.validate(testSuite, null);

      if (testCaseResult.failed()){
        LOGGER.warn("TestCleanup FAILED: test.name={}, testCase.name={}, failedAssertion={}, error={}",
            testCaseResult.integrationTest().meta().name(), "NULL", testCaseResult.failedAssertion().toString(), testCaseResult.error().toString());
      }
      else{
        LOGGER.info("TestCleanup PASSED: test.name={}, testCase.name={}",
            testCaseResult.integrationTest().meta().name(), "NULL");
      }
    }
  }

}
