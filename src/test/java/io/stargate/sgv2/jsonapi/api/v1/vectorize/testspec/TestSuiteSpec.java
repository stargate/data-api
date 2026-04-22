package io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec;



import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.TestAssertion;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;

public record TestSuiteSpec(
    TestSpecMeta meta, List<TestCommand> setup, List<TestCase> tests, List<TestCommand> cleanup)
    implements TestSpec {

  public DynamicContainer testNode(
          TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, List<TestRunEnv> allEnvs) {

    uriBuilder.addSegment(TestUri.Segment.SUITE, meta().name());

    var desc = "TestSuite: %s ".formatted(meta.name());
    var childs = allEnvs.stream()
            .map(testEnv -> testEnv.testNode(testNodeFactory, uriBuilder.clone(), this))
            .toList();
    return testNodeFactory.testPlanContainer(
        desc,
        uriBuilder.build().uri(),
            childs);
  }

  public List<? extends DynamicNode> testNodesForEnvironment(
          TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, TestRunEnv testEnvironment, TestExecutionCondition testExecutionCondition) {

    // not increasing the count of test nodes here, because this code is not actually making any
    // test nodes, it is all in things we call, they do the increasing
    List<DynamicNode> nodes = new ArrayList<>();

    int i = 1;
    var setupUriBuilder = uriBuilder.clone().addSegment(TestUri.Segment.STAGE, "setup");

    for (TestCommand setupCommand : setup()) {
      var setupRequest =
          new TestRunRequest(
              "SetupRequest[%s]: %s".formatted(i++, setupCommand.commandName()),
              setupCommand,
                  testNodeFactory.testPlan().target(),
              testEnvironment,
              TestAssertion.forSuccess(testNodeFactory.testPlan(), setupCommand),
              testExecutionCondition);

      nodes.add(setupRequest.testNodes(testNodeFactory, setupUriBuilder.clone()));
    }

    var testUriBuilder = uriBuilder.clone().addSegment(TestUri.Segment.STAGE, "test");
    for (var testCase : tests()) {
      nodes.add(
          testCase.testNodesForEnvironment(testNodeFactory, testUriBuilder.clone(), testEnvironment, testExecutionCondition));
    }

    // NOTE: For Cleanup we use a condition that is always TRUE because we always want to try to run a cleanup task.
    var alwaysTrueCondition = new TestExecutionCondition.AlwaysTrue("Cleanup Commands for parent URL: " + uriBuilder.build().uri().toString());
    var cleanupUriBuilder = uriBuilder.clone().addSegment(TestUri.Segment.STAGE, "cleanup");
    for (TestCommand cleanupCommand : cleanup()) {
      var cleanupRequest =
          new TestRunRequest(
              "CleanupRequest[%s]: %s".formatted(i++, cleanupCommand.commandName()),
              cleanupCommand,
                  testNodeFactory.testPlan().target(),
              testEnvironment,
              TestAssertion.forSuccess(testNodeFactory.testPlan(), cleanupCommand),
              alwaysTrueCondition);

      nodes.add(cleanupRequest.testNodes(testNodeFactory, cleanupUriBuilder.clone()));
    }

    return nodes;
  }

  public void expand(SpecFiles specFiles) {

    List<TestCommand> expandedSetup = new ArrayList<>();
    for (TestCommand command : setup) {
      if (command.includeFrom() != null) {
        var includedTest =
            specFiles
                .byNameAsType(TestSuiteSpec.class, command.includeFrom())
                .findFirst()
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Included TestSuite Setup not found. parent=%s, included=%s"
                                .formatted(meta().name(), command.includeFrom())));

        expandedSetup.addAll(includedTest.setup());
      } else {
        expandedSetup.add(command);
      }
    }
    setup.clear();
    setup.addAll(expandedSetup);

    List<TestCase> expandedTests = new ArrayList<>();
    for (TestCase testCase : tests) {
      if (testCase.include() != null) {
        var includedTest =
            specFiles
                .byNameAsType(TestSuiteSpec.class, testCase.include())
                .findFirst()
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Included TestSuite TestCase  not found. parent=%s, included=%s"
                                .formatted(meta().name(), testCase.include())));

        expandedTests.addAll(includedTest.tests());
      } else {
        expandedTests.add(testCase);
      }
    }
    tests.clear();
    tests.addAll(expandedTests);
  }
}
