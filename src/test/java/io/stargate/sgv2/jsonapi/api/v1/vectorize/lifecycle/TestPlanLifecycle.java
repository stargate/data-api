package io.stargate.sgv2.jsonapi.api.v1.vectorize.lifecycle;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestNodeFactory;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestRunEnv;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestUri;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.Job;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TestSuiteSpec;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.WorkflowSpec;
import java.util.Optional;
import org.junit.jupiter.api.DynamicNode;

public interface TestPlanLifecycle {

  default Optional<DynamicNode> beforeWorkflow(
          TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, WorkflowSpec workflow) {
    return Optional.empty();
  }

  default Optional<DynamicNode> afterWorkflow(
          TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, WorkflowSpec workflow) {
    return Optional.empty();
  }

  default Optional<DynamicNode> beforeJob(TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, Job job) {
    return Optional.empty();
  }

  default Optional<DynamicNode> afterJob(TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, Job job) {
    return Optional.empty();
  }

  default Optional<DynamicNode> beforeTestSuite(
          TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, TestSuiteSpec test, TestRunEnv env) {
    return Optional.empty();
  }

  default Optional<DynamicNode> afterTestSuite(
          TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, TestSuiteSpec test, TestRunEnv env) {
    return Optional.empty();
  }
}
