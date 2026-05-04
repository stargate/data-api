package io.stargate.sgv2.jsonapi.testbench.testrun;



import io.stargate.sgv2.jsonapi.testbench.assertions.TestAssertion;
import io.stargate.sgv2.jsonapi.testbench.targets.Target;
import io.stargate.sgv2.jsonapi.testbench.testspec.TestCommand;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;

public record TestRunRequest(
    String name,
    TestCommand testCommand,
    Target target,
    TestRunEnv testEnvironment,
    List<TestAssertion> testAssertions,
    TestExecutionCondition testExecutionCondition) {

  public TestRunResponse execute() {

    var apiRequest = target.apiRequest(testCommand, testEnvironment);
    return new TestRunResponse(this, apiRequest, apiRequest.execute());
  }

  public DynamicContainer testNodes(TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder) {

    uriBuilder.addSegment(TestUri.Segment.REQUEST, name());

    var nodes = new ArrayList<DynamicNode>();

    AtomicReference<TestRunResponse> atomicResponse = new AtomicReference<>();

    // Execute the request, and set  so the assertions can pull the response after.
    var commandExecutable =
        new DynamicTestExecutable(
            "Command: " + testCommand.commandName().getApiName(),
            uriBuilder
                .clone()
                .addSegment(TestUri.Segment.COMMAND, testCommand.commandName().getApiName()),
            testExecutionCondition,
            () -> atomicResponse.set(execute()));

    nodes.add(commandExecutable.testNode(testNodeFactory));

    // tests for each assertion
    var assertionsUriBuilder =
        uriBuilder.clone().addSegment(TestUri.Segment.ASSERTION_CONTAINER, "assertions");
    var assertionTests =
        testAssertions().stream()
            .map(
                testAssertion ->
                    testAssertion.testNodes(testNodeFactory, assertionsUriBuilder.clone(), atomicResponse, testExecutionCondition))
            .toList();

    // if we have assertion tests, put them in a container
    if (!assertionTests.isEmpty()) {
      nodes.add(
              testNodeFactory.testPlanContainer(
              "Assertions", assertionsUriBuilder.build().uri(), assertionTests));
    }

    return testNodeFactory.testPlanContainer("Request: " + name, uriBuilder.build().uri(), nodes);
  }
}
