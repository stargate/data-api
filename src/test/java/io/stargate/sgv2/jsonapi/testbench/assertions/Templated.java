package io.stargate.sgv2.jsonapi.testbench.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.testbench.TestPlan;
import io.stargate.sgv2.jsonapi.testbench.testspec.TestCommand;
import java.util.List;

public class Templated {

  static {
    AssertionFactory.REGISTRY.register(Templated.class);
  }

  public static List<TestAssertion> isSuccess(
      TestPlan testPlan, JsonNode template, TestCommand testCommand, JsonNode args) {
    var commandTemplate = template.get(testCommand.commandName().getApiName());
    if (commandTemplate == null) {
      throw new IllegalArgumentException(
          "isSuccess Assertion template not found for command: "
              + testCommand.commandName().getApiName());
    }
    return runTemplate(testPlan, (ObjectNode) commandTemplate, testCommand, args);
  }

  private static List<TestAssertion> runTemplate(
      TestPlan testPlan, ObjectNode template, TestCommand testCommand, JsonNode args) {
    return template.properties().stream()
        .map(entry -> new TestAssertion.AssertionDefinition(entry.getKey(), entry.getValue()))
        .map(def -> TestAssertion.buildAssertion(testPlan, testCommand, def))
        .toList();
  }
}
