package io.stargate.sgv2.jsonapi.testbench.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.testbench.TestBenchPlan;
import io.stargate.sgv2.jsonapi.testbench.testspec.TestCommand;
import java.util.List;

/**
 * Assertions that are defined by a JSON template.
 *
 * <p>There are two parts to using a template, first the template must be defined in a {@link
 * io.stargate.sgv2.jsonapi.testbench.testspec.TestSpecKind#ASSERTION_TEMPLATE} such as
 *
 * <pre>
 * {
 *   "meta": {
 *     "name": "assertions-templates",
 *     "kind": "assertion_template"
 *   },
 *   "templates": {
 *     "isSuccess": {
 *       "createCollection": {
 *         "http.success": null,
 *         "response.isDDLSuccess": null
 *       },
 *       "createKeyspace": {
 *         "http.success": null,
 *         "response.isDDLSuccess": null
 *       }
 *   }
 * }
 * </pre>
 *
 * <b>NOTE:</b> The format of a template is fixed: the members under "templates" as the names of the
 * template, the value is a JSON object that is passed to its factory below. Each template can then
 * have its own style of definition.
 *
 * <p>Strongly encouraged to use the same structure as isSucces: whose members are the names of API
 * commands, and those values are the assertions that should be run as you would define them
 * normally. We use this structure because the idea of a template like "isSuccess" is that it is
 * saying "whatever the API command we just ran it should be successful".
 *
 * <p>So, every template name from a config file like above must have a function here to create
 */
public class Templated {

  static {
    AssertionFactory.REGISTRY.register(Templated.class);
  }

  /** Assertion factory, see {@link AssertionFactory.TemplatedAssertionFactory} */
  public static List<TestAssertion> isSuccess(
      TestBenchPlan testPlan, JsonNode template, TestCommand testCommand, JsonNode args) {

    var commandTemplate = template.get(testCommand.commandName().getApiName());
    if (commandTemplate == null) {
      throw new IllegalArgumentException(
          "isSuccess Assertion template not found for command: "
              + testCommand.commandName().getApiName());
    }
    return runTemplate(testPlan, (ObjectNode) commandTemplate, testCommand, args);
  }

  private static List<TestAssertion> runTemplate(
      TestBenchPlan testPlan, ObjectNode template, TestCommand testCommand, JsonNode args) {

    return template.properties().stream()
        .map(entry -> new TestAssertion.AssertionDefinition(entry.getKey(), entry.getValue()))
        .map(def -> TestAssertion.buildAssertion(testPlan, testCommand, def))
        .toList();
  }
}
