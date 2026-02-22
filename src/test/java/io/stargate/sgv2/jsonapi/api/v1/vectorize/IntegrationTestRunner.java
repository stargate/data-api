package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.BodyAssertion;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.TestAssertion;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.restassured.RestAssured.given;

public class IntegrationTestRunner extends RunnerBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationTestRunner.class);


  // keyspace automatically created in this test
  protected static final String keyspaceName =
      "ks" + RandomStringUtils.insecure().nextAlphanumeric(16);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final ITCollection itCollection;
  private final IntegrationTarget target;
  private final IntegrationTest integrationTest;
  private final IntegrationEnv integrationEnv;

  public IntegrationTestRunner(
      ITCollection itCollection, IntegrationTarget target,  IntegrationTest integrationTest, IntegrationEnv integrationEnv) {
    this.itCollection = itCollection;
    this.target = target;
    this.integrationTest = integrationTest;
    this.integrationEnv = integrationEnv;
  }

  @Override
  protected IntegrationEnv integrationEnv() {
    return integrationEnv;
  }

  public void run() {

    LOGGER.info("Starting Integration Test with env={}", integrationEnv);
    for (TestCommand setupCommand : integrationTest.setup()) {
      var setupRequest = new TestRequest(setupCommand, target, integrationEnv, TestAssertion.forSuccess(setupCommand.commandName()));

      var setupResponse = setupRequest.execute();
      var testCaseResult = setupResponse.validate(integrationTest, null);

      if (testCaseResult.failed()){
        LOGGER.warn("TestSetup FAILED: test.name={}, testCase.name={}, failedAssertion={}, error={}",
            testCaseResult.integrationTest().meta().name(), "NULL", testCaseResult.failedAssertion(), String.valueOf(testCaseResult.error()));
      }
      else{
        LOGGER.info("TestSetup PASSED: test.name={}, testCase.name={}",
            testCaseResult.integrationTest().meta().name(), "NULL");
      }

    }

    for (TestCase testCase : integrationTest.tests()) {
      var testRequest = new TestRequest(testCase.command(), target, integrationEnv, TestAssertion.buildAssertions(testCase));
      var testResponse = testRequest.execute();
      var testCaseResult = testResponse.validate(integrationTest, testCase);

      if (testCaseResult.failed()){
        LOGGER.warn("TestCase FAILED: test.name={}, testCase.name={}, failedAssertion={}, error={}",
            testCaseResult.integrationTest().meta().name(),testCaseResult.testCase().name(), testCaseResult.failedAssertion(), String.valueOf(testCaseResult.error()));
      }
      else{
        LOGGER.info("TestCase PASSED: test.name={}, testCase.name={}",
            testCaseResult.integrationTest().meta().name(),testCaseResult.testCase().name());
      }
    }

    for (TestCommand cleanupCommand : integrationTest.cleanup()) {

      var cleanupRequest = new TestRequest(cleanupCommand, target, integrationEnv, TestAssertion.forSuccess(cleanupCommand.commandName()));

      var cleanupResponse = cleanupRequest.execute();
      var testCaseResult = cleanupResponse.validate(integrationTest, null);

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
