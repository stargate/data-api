package io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestRunEnv;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestPlan;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestRunRequest;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.TestAssertion;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestUri;
import org.junit.jupiter.api.DynamicContainer;

public record TestCase(

    String name,
    TestCommand command,
    ObjectNode asserts,
    @JsonProperty("$include")
    String include) {


    public DynamicContainer testNodesForEnvironment(TestPlan testPlan, TestUri.Builder uriBuilder, TestRunEnv testEnvironment) {

        var testRequest = new TestRunRequest(
            "TestCase: name=%s".formatted(name, command.commandName()),
            command(), testPlan.target(), testEnvironment, TestAssertion.buildAssertions(testPlan,  this));

        return testRequest.testNodes(uriBuilder);
    }

}
