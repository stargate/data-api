package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.TestAssertion;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

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

  public DynamicContainer testNodes() {

    List<DynamicNode> nodes = new ArrayList<>();
    AtomicReference<TestResponse> atomicResponse = new AtomicReference<>();

    // Execute the request, and set  so the assertions can pull the response after.
    nodes.add(dynamicTest("Command: " + testCommand.commandName().getApiName(), () -> atomicResponse.set(execute())));

    // tests for each assertion
    var assertionTests = testAssertions().stream().map(
        testAssertion -> testAssertion.testNodes(atomicResponse))
        .toList();

    // if we have assertion tests, put them in a container
    if (!assertionTests.isEmpty()) {
      nodes.add(dynamicContainer("Assertions", assertionTests));
    }

    return dynamicContainer("Request: " + name, nodes);
  }
}
