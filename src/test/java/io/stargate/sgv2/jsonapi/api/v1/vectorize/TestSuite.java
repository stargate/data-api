package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.AssertionMatcher;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.TestAssertion;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;

public record TestSuite(TestSpecMeta meta, List<TestCommand> setup, List<TestCase> tests, List<TestCommand> cleanup)
    implements TestSpec {

  @Override
  public TestSpecKind kind() {
    return TestSpecKind.TEST;
  }

  public DynamicContainer testNode(TestPlan testPlan, List<TestEnvironment> allEnvs) {

    var desc = "TestSuite: %s ".formatted(
        meta.name());

    return dynamicContainer(
            desc,
            allEnvs.stream()
                .map(testEnv -> testEnv.testNode(testPlan, this))
        );
  }

  Collection<? extends DynamicNode> testNodesForEnvironment(TestPlan  testPlan, TestEnvironment testEnvironment) {

      List<DynamicNode> nodes = new ArrayList<>();

      int i = 1;
      for (TestCommand setupCommand : setup()) {
        var setupRequest = new TestRequest(
            "SetupRequest[%s]: %s".formatted(i++, setupCommand.commandName()),
            setupCommand, testPlan.target(), testEnvironment, TestAssertion.forSuccess(setupCommand.commandName()));

        nodes.add(setupRequest.testNodes());
      }

    for (var testCase : tests()) {
      nodes.add(testCase.testNodesForEnvironment(testPlan, testEnvironment));
    }

    for (TestCommand cleanupCommand : cleanup()) {
      var cleanupRequest = new TestRequest(
          "CleanupRequest[%s]: %s".formatted(i++, cleanupCommand.commandName()),
          cleanupCommand, testPlan.target(), testEnvironment, TestAssertion.forSuccess(cleanupCommand.commandName()));
      nodes.add(cleanupRequest.testNodes());
    }

      return nodes;

  }
  public void expand(SpecFiles itCollection) {

    List<TestCommand> expandedSetup = new ArrayList<>();
    for (TestCommand command : setup) {
        if (command.includeFrom() != null){
          var includedTest = itCollection.testFirstByName(command.includeFrom());
          expandedSetup.addAll(includedTest.setup());
        }
        else {
          expandedSetup.add(command);
        }
    }
    setup.clear();
    setup.addAll(expandedSetup);

    List<TestCase> expandedTests = new ArrayList<>();
    for (TestCase item : tests) {
      if (item.include() != null ){
        var includedTest = itCollection.testFirstByName(item.include());
        expandedTests.addAll(includedTest.tests());
      }
      else {
        expandedTests.add(item);
      }
    }
    tests.clear();
    tests.addAll(expandedTests);
  }
}
