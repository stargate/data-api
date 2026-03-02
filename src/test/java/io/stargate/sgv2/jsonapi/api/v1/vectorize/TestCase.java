package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.AssertionMatcher;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.TestAssertion;
import org.junit.jupiter.api.DynamicContainer;

public record TestCase(

    String name,
    TestCommand command,
    ObjectNode asserts,
    @JsonProperty("$include")
    String include) {


    public DynamicContainer testNodesForEnvironment(TestPlan  testPlan, TestEnvironment testEnvironment) {

        var testRequest = new TestRequest(
            "TestCase: name=%s".formatted(name, command.commandName()),
            command(), testPlan.target(), testEnvironment, TestAssertion.buildAssertions(testPlan,  this));

        return testRequest.testNodes();
    }

}
