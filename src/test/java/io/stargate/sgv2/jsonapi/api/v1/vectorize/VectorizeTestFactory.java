package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public class VectorizeTestFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(VectorizeTestFactory.class);


  private static final int TEST_PARALLELISM = 8; // ← set this

  @TestFactory
  Stream<DynamicContainer> jobs() {

    var testPlan = TestPlan.create("local", List.of("all-vectorize-workflow"));

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
