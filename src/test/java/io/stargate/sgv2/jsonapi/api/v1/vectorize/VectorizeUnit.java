package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorizeUnit {
  private static final Logger LOGGER = LoggerFactory.getLogger(VectorizeUnit.class);

  @Test
  public void doTest() {

    var targets = Targets.loadAll("integration-tests/targets/targets.json");
    var integrationTarget = new IntegrationTarget(targets.target("local"));

    var itCollection = ITCollection.loadAll("integration-tests/vectorize");

    var workflow = itCollection.workflowFirstByName("all-vectorize-workflow");

    var jobs = workflow.jobs().stream()
        .filter(job -> !job.meta().tags().contains("disabled"))
        .toList();


    // SEQUENTIAL
    integrationTarget.workflowStarting(workflow);
    try {
      for (var job : jobs) {
        LOGGER.info("Starting job {}", job.meta());
        new IntegrationJobRunner(integrationTarget, itCollection, job).run();
      }
    } finally {
      integrationTarget.workflowFinished(workflow);
    }

      // Parallel
//    int maxConcurrentJobs = 2;
//    integrationTarget.workflowStarting(workflow);
//    try {
//      var executor = java.util.concurrent.Executors.newFixedThreadPool(maxConcurrentJobs);
//      try {
//        var futures = jobs.stream()
//            .map(job -> java.util.concurrent.CompletableFuture.runAsync(() -> {
//              LOGGER.info("Starting job {}", job.meta());
//              new IntegrationJobRunner(integrationTarget, itCollection, job).run();
//            }, executor))
//            .toList();
//
//        // wait for all, fail if any failed
//        futures.forEach(java.util.concurrent.CompletableFuture::join);
//      } finally {
//        executor.shutdown();
//      }
//    } finally {
//      integrationTarget.workflowFinished(workflow);
//    }

  }
}
