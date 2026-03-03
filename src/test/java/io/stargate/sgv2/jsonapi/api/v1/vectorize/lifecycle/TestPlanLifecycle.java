package io.stargate.sgv2.jsonapi.api.v1.vectorize.lifecycle;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.Job;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestEnvironment;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestPlan;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TestSuite;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TestUri;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.Workflow;
import org.junit.jupiter.api.DynamicNode;

import java.util.Optional;

public interface TestPlanLifecycle {

  default Optional<DynamicNode> beforeWorkflow(TestPlan testPlan, TestUri.Builder uriBuilder, Workflow workflow){
    return Optional.empty();
  }
  default Optional<DynamicNode>  afterWorkflow(TestPlan testPlan,TestUri.Builder uriBuilder, Workflow workflow){
    return Optional.empty();
  }

  default Optional<DynamicNode>  beforeJob(TestPlan testPlan,TestUri.Builder uriBuilder, Job job){
    return Optional.empty();
  }
  default Optional<DynamicNode>  afterJob(TestPlan testPlan, TestUri.Builder uriBuilder, Job job){
    return Optional.empty();
  }

  default Optional<DynamicNode>  beforeTestSuite(TestPlan testPlan, TestUri.Builder uriBuilder,TestSuite test, TestEnvironment env){
    return Optional.empty();
  }
  default Optional<DynamicNode>  afterTestSuite(TestPlan testPlan, TestUri.Builder uriBuilder,TestSuite test, TestEnvironment env){
    return Optional.empty();
  }

}
