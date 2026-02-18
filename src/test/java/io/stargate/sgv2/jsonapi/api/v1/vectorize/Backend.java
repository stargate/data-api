package io.stargate.sgv2.jsonapi.api.v1.vectorize;

public abstract class Backend {

  public void workflowStarting(IntegrationTarget integrationTarget, IntegrationWorkflow workflow) { }

  public void workflowFinished(IntegrationTarget integrationTarget, IntegrationWorkflow workflow) { }

  public void jobStarting(IntegrationTarget integrationTarget, IntegrationJob job) { }

  public void jobFinished(IntegrationTarget integrationTarget, IntegrationJob job) { }

  public void testStarting(IntegrationTarget integrationTarget, IntegrationTest test, IntegrationEnv env) { }

  public void testFinished(IntegrationTarget integrationTarget, IntegrationTest test, IntegrationEnv env) { }
}

