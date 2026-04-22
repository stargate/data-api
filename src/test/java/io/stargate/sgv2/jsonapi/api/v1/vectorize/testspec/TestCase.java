package io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.TestAssertion;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.*;
import org.junit.jupiter.api.DynamicContainer;

public record TestCase(
    String name,
    TestCommand command,
    ObjectNode asserts,
    @JsonProperty("$include") String include) {

  public DynamicContainer testNodesForEnvironment(
          TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, TestRunEnv testEnvironment, TestExecutionCondition testExecutionCondition) {

    var testRequest =
        new TestRunRequest(
            "TestCase: name=%s".formatted(name, command.commandName()),
            command(),
            testNodeFactory.testPlan().target(),
            testEnvironment,
            TestAssertion.buildAssertions(testNodeFactory.testPlan(), this),
            testExecutionCondition);

    return testRequest.testNodes(testNodeFactory, uriBuilder);
  }
}
