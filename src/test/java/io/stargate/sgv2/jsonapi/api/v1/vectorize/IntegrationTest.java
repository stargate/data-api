package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import java.util.ArrayList;
import java.util.List;

public record IntegrationTest(ITMetadata meta, List<TestCommand> setup, List<TestCase> tests, List<TestCommand> cleanup)
    implements ITElement {

  @Override
  public ITElementKind kind() {
    return ITElementKind.TEST;
  }

  public void expand(ITCollection itCollection) {

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
