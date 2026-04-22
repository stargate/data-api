package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestExecutionCondition;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestRunResponse;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestUri;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DynamicNode;

public record TestAssertionContainer(String name, JsonNode args, List<TestAssertion> assertions)
    implements TestAssertion {

  @Override
  public void run(TestRunResponse testResponse) {
    for (TestAssertion assertion : assertions) {
      try {
        assertion.run(testResponse);
      } catch (AssertionError e) {
        System.out.printf(
            "Failed Assertion Container: name=%s, args=%s\n\t Caused By: name=%s, args=%s",
            name, args, assertion.name(), assertion.args());
        throw e;
      } catch (Exception e) {
        System.out.printf(
            "Error In Assertion Container: name=%s, args=%s\n\t Caused By: name=%s, args=%s",
            name, args, assertion.name(), assertion.args());
        throw e;
      }
    }
  }

  @Override
  public DynamicNode testNodes(
      TestUri.Builder uriBuilder, AtomicReference<TestRunResponse> testResponse, TestExecutionCondition testExecutionCondition) {

    uriBuilder.addSegment(TestUri.Segment.ASSERTION, name());
    var childs =
        assertions.stream()
            .map(assertion -> assertion.testNodes(uriBuilder.clone(), testResponse, testExecutionCondition))
            .toList();

    return dynamicContainer(name, childs);
  }
}
