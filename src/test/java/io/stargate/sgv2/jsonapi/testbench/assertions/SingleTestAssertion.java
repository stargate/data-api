package io.stargate.sgv2.jsonapi.testbench.assertions;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.concurrent.atomic.AtomicReference;

import io.stargate.sgv2.jsonapi.testbench.testrun.*;
import org.junit.jupiter.api.DynamicNode;

public record SingleTestAssertion(String name, JsonNode args, AssertionMatcher matcher)
    implements TestAssertion {

  public void run(TestRunResponse testResponse) {

    try {
      matcher.match(testResponse.apiResponse());
    } catch (AssertionError e) {
//      System.out.printf("Failed Assertion: name=%s, args=%s", name, args);
      throw e;
    } catch (Exception e) {
//      System.out.printf("Error In Assertion: name=%s, args=%s", name, args);
      throw e;
    }
  }

  @Override
  public DynamicNode testNodes(
          TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, AtomicReference<TestRunResponse> testResponse, TestExecutionCondition testExecutionCondition) {

    var matcherDesc = (matcher instanceof Describable d) ? d.describe() : "";

    var executable =
        new DynamicTestExecutable(
            "%s [%s]".formatted(name(), matcherDesc),
            uriBuilder.addSegment(TestUri.Segment.ASSERTION, name()),
            testExecutionCondition,
            () -> {
              var resp = testResponse.get();
              if (resp == null) {
                throw new IllegalStateException("Response is null");
              }
              run(resp);
            });

    return executable.testNode(testNodeFactory);
  }
}
