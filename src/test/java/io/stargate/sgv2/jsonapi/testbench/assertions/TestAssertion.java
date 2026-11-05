package io.stargate.sgv2.jsonapi.testbench.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.testbench.TestBenchPlan;
import io.stargate.sgv2.jsonapi.testbench.testrun.*;
import io.stargate.sgv2.jsonapi.testbench.testspec.AssertionTemplateSpec;
import io.stargate.sgv2.jsonapi.testbench.testspec.TestCase;
import io.stargate.sgv2.jsonapi.testbench.testspec.TestCommand;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicNode;

/**
 * A single assertion to run on the result of a single request sent to the API. This is the
 * assertion with name, description etc, and the logic of the matcher that will perform the test.
 * See also {@link AssertionMatcher}
 *
 * <p>Assertions are defined in code, and then linked to the setup, test, or cleanup request in the
 * test-suite. Or in the case of Target lifecycle (such as creating a keyspace) created in code
 * entirely.
 *
 * <p>For example, this TestCase has two assertions. One is templated, that is made up of multiple
 * other assertions, and the other is a simple matcher.
 *
 * <pre>
 *     {
 *       "name": "basic findMany",
 *       "command": {
 *         "find": {
 *           "sort": {
 *             "$vectorize": "I love movies!"
 *           }
 *         }
 *       },
 *       "asserts": {
 *         "Templated.isSuccess": null,
 *         "Documents.count": 3
 *       }
 *     }
 * </pre>
 *
 * <p><b>NOTE:</b> The name of the assertion, e.g. "Documents.count" MUST to the name of a
 * "Class.Method" in the "assertions" package. For example see {@link Documents#count(TestCommand,
 * JsonNode)} and the {@link AssertionFactory} for the registry of assertions. The assertion object
 * for a particular assertion, that is an instance with the configuration from above, is created in
 * {@link AssertionDefWithFactory#build(TestBenchPlan, TestCommand)}
 */
public sealed interface TestAssertion permits SingleTestAssertion, TestAssertionContainer {

  /** Friendly name used in logs and reports. */
  String name();

  /**
   * Arguments that may be passed to the assertion from the test suite definition. For example, text
   * node <code>3</code> will be passed to "Documents.count" in the above. Used for reporting /
   * logging.
   */
  JsonNode args();

  /**
   * Called for the assertion to run against the response of running the API request.
   *
   * <p>Three things can happen:
   *
   * <ol>
   *   <li>Everything is OK, returns without exception.
   *   <li>The assertion fails, throws an exception like a normal Junit test, and it will be
   *       recorded as a failure.
   *   <li>Throw a {@link org.junit.AssumptionViolatedException} to say the assumption was not meet
   *       and the test is aborted. This is normally done before the assertion is called so we do
   *       not send requests after one has failed in the test env, see {@link
   *       io.stargate.sgv2.jsonapi.testbench.testrun.DynamicTestExecutable}.
   * </ol>
   *
   * @param testResponse
   */
  void run(TestRunResponse testResponse);

  /**
   * Gets {@link DynamicNode} that represents this assertion in the test tree.
   *
   * <p>An assertion is always a single test node in the test tree, not a container. See {@link
   * SingleTestAssertion} for the implementation for a single assertion. Where we have a group of
   * assertions, they end up going to that class as well to get the test nodes for the individual
   * assertions.
   *
   * @param testNodeFactory Factory to build test nodes with, for common naming etc.
   * @param uriBuilder Builder for the URI tp descrbie the type of node in the test tree.
   * @param testResponse Atomic reference that will be updated with the response of the test
   *     request, that the assertion will use to perform to rune once it is time to execute.
   * @param testExecutionCondition Condition that can be checked to see if the test node should be
   *     skipped.
   * @return DynamicNode representing this assertion in the test tree, may be a container if there
   *     are multiple assertions.
   */
  DynamicNode testNodes(
      TestNodeFactory testNodeFactory,
      TestUri.Builder uriBuilder,
      AtomicReference<TestRunResponse> testResponse,
      TestExecutionCondition testExecutionCondition);

  /**
   * Returns a list of assertions that can be used to determine if a command was successful.
   *
   * <p>Uses the <code>Templated.isSuccess</code> templated assertion.
   */
  static List<TestAssertion> forSuccess(TestBenchPlan testPlan, TestCommand testCommand) {

    var builder =
        Stream.<AssertionDefinition>builder()
            .add(new AssertionDefinition("Templated.isSuccess", null));

    return buildAssertions(testPlan, testCommand, builder.build());
  }

  /**
   * Creates assertions based on the configuration of the {@link TestCase}, that is what is under
   * the "asserts" member in the example above.
   */
  static List<TestAssertion> buildAssertions(TestBenchPlan testPlan, TestCase testCase) {

    var defs = testCase.asserts().properties().stream().map(AssertionDefinition::create);
    return buildAssertions(testPlan, testCase.command(), defs);
  }

  static List<TestAssertion> buildAssertions(
      TestBenchPlan testPlan, TestCommand testCommand, Stream<AssertionDefinition> defs) {

    return defs.map(def -> buildAssertion(testPlan, testCommand, def)).toList();
  }

  static TestAssertion buildAssertion(
      TestBenchPlan testPlan, TestCommand testCommand, AssertionDefinition def) {
    return def.addFactory(AssertionFactory.REGISTRY).build(testPlan, testCommand);
  }

  /**
   * Definition of an assertion that can be used to create an instance of the assertion used that
   * can be run later.
   *
   * <p>Example, the member and it's value in below:
   *
   * <pre>
   *     {
   *         "Documents.count": 3
   *     }
   * </pre>
   *
   * @param name Name of the assertion, it *must* map to a class and method in the "assertions"
   *     package.
   * @param args Arguments that will be passed to the assertion factory, see {@link
   *     AssertionFactory}
   */
  record AssertionDefinition(String name, JsonNode args) {

    static AssertionDefinition create(Map.Entry<String, JsonNode> def) {
      return new AssertionDefinition(def.getKey(), def.getValue());
    }

    AssertionDefWithFactory addFactory(AssertionFactoryRegistry registry) {

      var factory = registry.getWrappedAssertionFactory(name());
      if (factory == null) {
        throw new IllegalStateException("Unknown Assertion Factory name=" + name());
      }
      return new AssertionDefWithFactory(factory, args);
    }
  }

  /**
   * Definition of an assertion where we have the factory function that can create it with the
   * arguments supplied by the test definition.
   */
  record AssertionDefWithFactory(AssertionFactory.WrappedMethod method, JsonNode args) {

    /**
     * Create an assertion instance by calling the appropriate factory function with the arguments
     * from the test definition.
     *
     * @param testPlan the test plan that is being created holds context of what we are doing.
     * @param testCommand The actual command that will be executed, that we will want to assert the
     *     result of
     * @return A single assertion that can be run later.
     */
    TestAssertion build(TestBenchPlan testPlan, TestCommand testCommand) {

      return switch (method) {
          // basic single assertion
        case AssertionFactory.WrappedAssertionMatcherFactory factory ->
            new SingleTestAssertion(
                method.properName(), args(), factory.create(testCommand, args()));

          // templated, we need to look up what assertions should be in it.
        case AssertionFactory.TemplatedAssertionFactory factory -> {
          // search all assertion templates to find any that have the name given in the test
          // definition, there
          // must be one and only one
          var template =
              testPlan
                  .specFiles()
                  .byType(AssertionTemplateSpec.class)
                  .flatMap(
                      assertTemplate -> assertTemplate.templateFor(method.properName()).stream())
                  .reduce(
                      (a, b) -> {
                        throw new IllegalStateException(
                            "Multiple Assertion Templates found for name=" + method.properName());
                      })
                  .orElseThrow(
                      () ->
                          new IllegalStateException(
                              "Unknown Assertion Template name=" + method.properName()));

          // templated assertions have children, so we need an assertion that contains those
          // children.
          // the factory we call here will use the template to make AssertionDefinition 's and end
          // up back in this method to make the childred
          yield new TestAssertionContainer(
              method.properName(), args(), factory.create(testPlan, template, testCommand, args()));
        }
      };
    }
  }
}
