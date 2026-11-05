package io.stargate.sgv2.jsonapi.testbench.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestExecutionCondition;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestNodeFactory;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestRunResponse;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestUri;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DynamicNode;

/**
 * An assertion made up of child assertions, for example built from a template that created multiple
 * assertions
 *
 * @param name Name of the template, e.g. "Templated.isSuccess"
 * @param args Raw arguments passed into the template factory, from the test case definition
 * @param assertions Child assertions that were created by the template factory
 */
public record TestAssertionContainer(String name, JsonNode args, List<TestAssertion> assertions)
    implements TestAssertion {

  @Override
  public void run(TestRunResponse testResponse) {

    // we are just a container, let those exceptions bubble up
    assertions.forEach(assertion -> assertion.run(testResponse));
  }

  @Override
  public DynamicNode testNodes(
      TestNodeFactory testNodeFactory,
      TestUri.Builder uriBuilder,
      AtomicReference<TestRunResponse> testResponse,
      TestExecutionCondition testExecutionCondition) {

    uriBuilder.addSegment(TestUri.Segment.ASSERTION, name());

    var children =
        assertions.stream()
            .map(
                assertion ->
                    assertion.testNodes(
                        testNodeFactory, uriBuilder.clone(), testResponse, testExecutionCondition))
            .toList();

    return testNodeFactory.testPlanContainer(name, uriBuilder.build().uri(), children);
  }
}
