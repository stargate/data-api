package io.stargate.sgv2.jsonapi.testbench.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.testbench.testrun.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DynamicNode;

/**
 * A single test assertion that performs a single test of the results of a request.
 *
 * @param name Name of the assertion, e.g. "Documents.count"
 * @param args Raw arguments passed into the assertion factory, from the test case definition
 * @param matcher The actual logic that will be run to assert the result of the request.
 */
public record SingleTestAssertion(String name, JsonNode args, AssertionMatcher matcher)
    implements TestAssertion {

  @Override
  public void run(TestRunResponse testResponse) {
    // exceptions can bubble out, that's how the frameworks know the assertion result.
    matcher.match(testResponse.apiResponse());
  }

  @Override
  public DynamicNode testNodes(
      TestNodeFactory testNodeFactory,
      TestUri.Builder uriBuilder,
      AtomicReference<TestRunResponse> testResponse,
      TestExecutionCondition testExecutionCondition) {

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
