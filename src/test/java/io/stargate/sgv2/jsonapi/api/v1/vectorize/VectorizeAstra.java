package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Stream;

public class VectorizeAstra {
  private static final Logger LOGGER = LoggerFactory.getLogger(VectorizeAstra.class);


  private static final int TEST_PARALLELISM = 8; // ← set this

  @TestFactory
  Stream<DynamicContainer> jobs() {

    var testPlan = TestPlan.create("astra-dev", List.of("all-vectorize-workflow"), false);

    return testPlan.testNode();

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
