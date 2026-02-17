package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.AbstractCollectionIntegrationTestBase;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.Test;

public class VectorizeUnit  {

  @Test
  public void doTest() {

    var itCollection = ITCollection.loadAll("integration-tests/vectorize");

    var workflow = itCollection.workflowFirstByName("all-vectorize-workflow");

    var job = workflow.jobs().getLast();

    new IntegrationJobRunner(itCollection, job).run();
  }
}
