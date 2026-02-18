package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.AbstractCollectionIntegrationTestBase;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
public class VectorizeIT extends AbstractCollectionIntegrationTestBase {

  @Test
  public void doTest() {

//    var itCollection = ITCollection.loadAll("integration-tests/vectorize");
//
//    var workflow = itCollection.workflowFirstByName("all-vectorize-workflow");
//
//    var job = workflow.jobs().getFirst();
//
//    new IntegrationJobRunner(itCollection, job).run();
  }
}
