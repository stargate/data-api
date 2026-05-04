package io.stargate.sgv2.jsonapi.testbench.lifecycle;

import io.stargate.sgv2.jsonapi.testbench.testrun.TestNodeFactory;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestRunEnv;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestUri;
import io.stargate.sgv2.jsonapi.testbench.testspec.Job;
import io.stargate.sgv2.jsonapi.testbench.testspec.TestSuiteSpec;
import io.stargate.sgv2.jsonapi.testbench.testspec.WorkflowSpec;
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
