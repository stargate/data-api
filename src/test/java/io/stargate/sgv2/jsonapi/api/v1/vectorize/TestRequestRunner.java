package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import org.junit.jupiter.api.DynamicTest;

import java.util.Collection;
import java.util.Objects;

import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class TestRequestRunner extends RunnerBase{

  private final TestRequest testRequest;
  private final TestSuite testSuite;
  private final TestCase testCase;

  public  TestRequestRunner(TestRequest testRequest, TestSuite testSuite, TestCase testCase ) {
    this.testRequest = Objects.requireNonNull(testRequest, "testRequest must not be null");

    this.testSuite = testSuite;
    this.testCase = testCase;
  }

  @Override
  public void execute() throws Throwable {

//    testRequest.execute().validate(testSuite, testCase);
  }

}
