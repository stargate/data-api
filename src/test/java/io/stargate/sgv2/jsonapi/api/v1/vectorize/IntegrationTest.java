package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import java.util.ArrayList;
import java.util.List;

public record IntegrationTest(ITMetadata meta, List<TestRequest> setup, List<TestItem> tests)
    implements ITElement {

  @Override
  public ITElementKind kind() {
    return ITElementKind.TEST;
  }

  public void expand(ITCollection itCollection) {

    List<TestRequest> expandedSetup = new ArrayList<>();
    for (TestRequest request : setup) {
        if (request.request().has("$include")){
          var includedTest = itCollection.testsFirstByName(request.request().get("$include").textValue());
          expandedSetup.addAll(includedTest.setup());
        }
        else {
          expandedSetup.add(request);
        }
    }
    setup.clear();
    setup.addAll(expandedSetup);

    List<TestItem> expandedTests = new ArrayList<>();
    for (TestItem item : tests) {
      if (item.include() != null ){
        var includedTest = itCollection.testsFirstByName(item.include());
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
