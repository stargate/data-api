package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.SpecFiles;

public class IntegrationJobRunner {

  private final Target target;
  private final SpecFiles itCollection;
  private final Job job;

  public IntegrationJobRunner(Target target, SpecFiles itCollection, Job job) {
    this.target = target;
    this.itCollection = itCollection;
    this.job = job;
  }

//  public void run() {
//
//    target.jobStarting(job);
//    var allEnvs = job.allEnvironments();
//
//    List<TestSuite> allTests = new ArrayList<>();
//    job.tests()
//        .forEach(
//            testName -> {
//              allTests.addAll(itCollection.testByName(testName));
//            });
//
//    try{
//
//      for (TestSuite test : allTests) {
//        for (TestEnvironment env : allEnvs) {
//
//          try{
//            target.testStarting(test, env);
//            var testRunner = new IntegrationTestRunner( itCollection, target, test, env);
//            testRunner.run();
//          }
//          finally{
//            target.testFinished(test, env);
//          }
//
//        }
//      }
//    }
//    finally {
//      target.jobFinished(job);
//    }
//  }
}
