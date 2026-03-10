package io.stargate.sgv2.jsonapi.api.v1.vectorize.targets;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.Job;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.lifecycle.TestPlanLifecycle;

public abstract class Backend implements TestPlanLifecycle {

  public void updateJobForTarget(Job job){
  }

}

