package io.stargate.sgv2.jsonapi.api.v1.vectorize;

public abstract class Backend {

  public void updateJobForTarget(Job job){
  }

  public void workflowStarting(TestPlan testPlan, Workflow workflow) { }

  public void workflowFinished(TestPlan testPlan, Workflow workflow) { }

  public void jobStarting(TestPlan testPlan, Job job) { }

  public void jobFinished(TestPlan testPlan, Job job) { }

  public void testStarting(TestPlan testPlan, TestSuite test, TestEnvironment env) { }

  public void testFinished(TestPlan testPlan, TestSuite test, TestEnvironment env) { }
}

