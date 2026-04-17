package io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec;

import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.*;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.TestAssertion;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestRunEnv;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestRunRequest;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestUri;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;

public record TestSuiteSpec(
    TestSpecMeta meta, List<TestCommand> setup, List<TestCase> tests, List<TestCommand> cleanup)
    implements TestSpec {

  public DynamicContainer testNode(
      TestPlan testPlan, TestUri.Builder uriBuilder, List<TestRunEnv> allEnvs) {

    uriBuilder.addSegment(TestUri.Segment.SUITE, meta().name());

    var desc = "TestSuite: %s ".formatted(meta.name());

    return dynamicContainer(
        desc,
        uriBuilder.build().uri(),
        allEnvs.stream().map(testEnv -> testEnv.testNode(testPlan, uriBuilder.clone(), this)));
  }

  public Collection<? extends DynamicNode> testNodesForEnvironment(
      TestPlan testPlan, TestUri.Builder uriBuilder, TestRunEnv testEnvironment) {

    List<DynamicNode> nodes = new ArrayList<>();

    int i = 1;
    var setupUriBuilder = uriBuilder.clone().addSegment(TestUri.Segment.STAGE, "setup");

    for (TestCommand setupCommand : setup()) {
      var setupRequest =
          new TestRunRequest(
              "SetupRequest[%s]: %s".formatted(i++, setupCommand.commandName()),
              setupCommand,
              testPlan.target(),
              testEnvironment,
              TestAssertion.forSuccess(testPlan, setupCommand));

      nodes.add(setupRequest.testNodes(setupUriBuilder.clone()));
    }

    var testUriBuilder = uriBuilder.clone().addSegment(TestUri.Segment.STAGE, "test");
    for (var testCase : tests()) {
      nodes.add(
          testCase.testNodesForEnvironment(testPlan, testUriBuilder.clone(), testEnvironment));
    }

    var cleanupUriBuilder = uriBuilder.clone().addSegment(TestUri.Segment.STAGE, "cleanup");
    for (TestCommand cleanupCommand : cleanup()) {
      var cleanupRequest =
          new TestRunRequest(
              "CleanupRequest[%s]: %s".formatted(i++, cleanupCommand.commandName()),
              cleanupCommand,
              testPlan.target(),
              testEnvironment,
              TestAssertion.forSuccess(testPlan, cleanupCommand));
      nodes.add(cleanupRequest.testNodes(cleanupUriBuilder.clone()));
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
