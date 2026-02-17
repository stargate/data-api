package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import java.util.ArrayList;
import java.util.List;

public class IntegrationJobRunner {

  private final ITCollection itCollection;
  private final IntegrationJob job;

  public IntegrationJobRunner(ITCollection itCollection, IntegrationJob job) {
    this.itCollection = itCollection;
    this.job = job;
  }

  public void run() {

    var allEnvs = job.allEnvironments();

    List<IntegrationTest> allTests = new ArrayList<>();
    job.tests()
        .forEach(
            testName -> {
              allTests.addAll(itCollection.testsByName(testName));
            });

    for (IntegrationTest test : allTests) {
      for (IntegrationEnv env : allEnvs) {
        var testRunner = new IntegrationTestRunner(itCollection, test, env);
        testRunner.run();
      }
    }
  }
}
