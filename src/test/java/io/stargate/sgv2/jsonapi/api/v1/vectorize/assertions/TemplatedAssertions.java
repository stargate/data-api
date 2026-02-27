package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestCommand;

import java.util.List;

public class TemplatedAssertions {

  private static final AssertionTemplates assertions = AssertionTemplates.load();

  public static AssertionMatcher.TestAssertionContainerFactory getFactory(String templateName){

    JsonNode template = null;
    for (var entry : assertions.templates().properties()){
      if (entry.getKey().equalsIgnoreCase(templateName)) {
        template = entry.getValue();
        break;
      }
    }

    if (template == null) {
      throw new IllegalArgumentException("Assertion template not found: " + templateName);
    }
    if (! (template instanceof ObjectNode templateObject)){
      throw  new IllegalArgumentException("Assertion template is not an object: " + templateName);
    }

    return switch (templateName.toLowerCase()) {
      case "issuccess" ->
          (AssertionMatcher.TestAssertionContainerFactory) (testCommand, args) -> {
            var commandTemplate = templateObject.get(testCommand.commandName().getApiName());
            if (commandTemplate == null) {
              throw new IllegalArgumentException(
                  "isSuccess Assertion template not found for command: "
                      + testCommand.commandName().getApiName());
            }
            return runTemplate((ObjectNode) commandTemplate, testCommand, args);
          };
      default ->
          throw new IllegalArgumentException(
              "Assertion template not found: " + templateName);
    };
  }

  private static List<TestAssertion> runTemplate(ObjectNode template, TestCommand testCommand, JsonNode args) {
    return template.properties().stream()
            .map(entry -> new TestAssertion.AssertionDefinition(entry.getKey(), entry.getValue()))
            .map(def -> TestAssertion.buildAssertion(testCommand, def))
            .toList();
  }

}
