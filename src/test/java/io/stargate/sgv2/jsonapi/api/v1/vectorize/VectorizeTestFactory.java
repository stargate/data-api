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
  @TestFactory
  Stream<DynamicContainer> jobs() {

    var testPlan = TestPlan.create("local", List.of("all-vectorize-workflow"));

    return testPlan.testNode();
  }
}
