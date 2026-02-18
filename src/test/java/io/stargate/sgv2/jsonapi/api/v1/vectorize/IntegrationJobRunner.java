package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import java.util.ArrayList;
import java.util.List;

public class IntegrationJobRunner {

  private final IntegrationTarget target;
  private final ITCollection itCollection;
  private final IntegrationJob job;

  public IntegrationJobRunner( IntegrationTarget target, ITCollection itCollection, IntegrationJob job) {
    this.target = target;
    this.itCollection = itCollection;
    this.job = job;
  }

  public void run() {

    target.jobStarting(job);
    var allEnvs = job.allEnvironments();

    List<IntegrationTest> allTests = new ArrayList<>();
    job.tests()
        .forEach(
            testName -> {
              allTests.addAll(itCollection.testByName(testName));
            });

    try{

      for (IntegrationTest test : allTests) {
        for (IntegrationEnv env : allEnvs) {

          try{
            target.testStarting(test, env);
            var testRunner = new IntegrationTestRunner( itCollection, target, test, env);
            testRunner.run();
          }
          finally{
            target.testFinished(test, env);
          }

        }
      }
    }
    finally {
      target.jobFinished(job);
    }
  }
}
