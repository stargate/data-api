package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TestSuite;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.Workflow;
import org.junit.jupiter.api.DynamicNode;

import java.util.Optional;

public abstract class Backend {

  public void updateJobForTarget(Job job){
  }

  public Optional<DynamicNode> beforeWorkflow(TestPlan testPlan, Workflow workflow) {
    return Optional.empty();
  }

  public Optional<DynamicNode> afterWorkflow(TestPlan testPlan, Workflow workflow) {
    return Optional.empty();
  }

  public Optional<DynamicNode> beforeJob(TestPlan testPlan, Job job) {
    return Optional.empty();
  }

  public Optional<DynamicNode> afterJob(TestPlan testPlan, Job job) {
    return Optional.empty();
  }

  public Optional<DynamicNode> beforeTestSuite(TestPlan testPlan, TestSuite test, TestEnvironment env) {
    return Optional.empty();
  }

  public Optional<DynamicNode> afterTestSuite(TestPlan testPlan, TestSuite test, TestEnvironment env) {
    return Optional.empty();
  }
}

