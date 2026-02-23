package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import org.junit.jupiter.api.DynamicContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

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
