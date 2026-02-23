package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.AbstractCollectionIntegrationTestBase;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.List;
import java.util.stream.Stream;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
public class VectorizeIT extends AbstractCollectionIntegrationTestBase {

  @TestFactory
  Stream<DynamicContainer> jobs() {

    var testPlan = TestPlan.create("integration", List.of("all-vectorize-workflow"));

    return testPlan.testNode();
  }
}
