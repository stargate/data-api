package io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.TestAssertion;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.targets.Target;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TestCommand;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public record TestRunRequest(
    String name,
    TestCommand testCommand,
    Target target,
    TestRunEnv testEnvironment,
    List<TestAssertion> testAssertions) {

  public TestRunResponse execute() {

    var apiRequest = target.apiRequest(testCommand, testEnvironment);
    return new TestRunResponse(this, apiRequest, apiRequest.execute());
  }

  public DynamicContainer testNodes(TestUri.Builder uriBuilder) {

    uriBuilder.addSegment(TestUri.Segment.REQUEST, name());

    Stream.Builder<DynamicNode> nodesBuilder = Stream.builder();;

    AtomicReference<TestRunResponse> atomicResponse = new AtomicReference<>();

    // Execute the request, and set  so the assertions can pull the response after.
    var commandExecutable = new DynamicTestExecutable(
        "Command: " + testCommand.commandName().getApiName(),
        uriBuilder.clone().addSegment(TestUri.Segment.COMMAND, testCommand.commandName().getApiName()),
        () -> atomicResponse.set(execute())
    );
    nodesBuilder.add(commandExecutable.testNode());

    // tests for each assertion
    var assertionsUriBuilder = uriBuilder.clone().addSegment(TestUri.Segment.ASSERTION_CONTAINER, "assertions");
    var assertionTests = testAssertions().stream().map(
        testAssertion -> testAssertion.testNodes(assertionsUriBuilder.clone(), atomicResponse))
        .toList();

    // if we have assertion tests, put them in a container
    if (!assertionTests.isEmpty()) {
      nodesBuilder.add(dynamicContainer("Assertions",assertionsUriBuilder.build().uri(), assertionTests.stream()));
    }

    return dynamicContainer("Request: " + name, uriBuilder.build().uri(), nodesBuilder.build());
  }
}
