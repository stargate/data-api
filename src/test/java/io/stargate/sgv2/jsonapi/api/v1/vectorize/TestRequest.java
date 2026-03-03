package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.TestAssertion;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TestUri;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public record TestRequest(
    String name,
    TestCommand testCommand,
    Target target,
    TestEnvironment testEnvironment,
    List<TestAssertion> testAssertions) {

  public TestResponse execute() {

    var apiRequest = target.apiRequest(testCommand, testEnvironment);
    return new TestResponse(this, apiRequest, apiRequest.execute());
  }

  public DynamicContainer testNodes(TestUri.Builder uriBuilder) {

    uriBuilder.addSegment(TestUri.Segment.REQUEST, name());

    Stream.Builder<DynamicNode> nodesBuilder = Stream.builder();;

    AtomicReference<TestResponse> atomicResponse = new AtomicReference<>();

    // Execute the request, and set  so the assertions can pull the response after.
    var commandUriBuilder = uriBuilder.clone();
    commandUriBuilder.addSegment(TestUri.Segment.COMMAND, testCommand.commandName().getApiName());
    nodesBuilder.add(dynamicTest("Command: " + testCommand.commandName().getApiName(),
        commandUriBuilder.build().uri(),
        () -> atomicResponse.set(execute())));

    // tests for each assertion
    var assertionsUriBuilder = uriBuilder.clone().addSegment(TestUri.Segment.ASSERTION_CONTAINER, "assertions");
    var assertionTests = testAssertions().stream().map(
        testAssertion -> testAssertion.testNodes(assertionsUriBuilder.clone(), atomicResponse))
        .toList();

    // if we have assertion tests, put them in a container
    if (!assertionTests.isEmpty()) {
      nodesBuilder.add(dynamicContainer("Assertions", assertionTests));
    }

    return dynamicContainer("Request: " + name, uriBuilder.build().uri(), nodesBuilder.build());
  }
}
