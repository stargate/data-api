package io.stargate.sgv2.jsonapi.api.v1.vectorize.backends;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.Job;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.lifecycle.TestPlanLifecycle;

public abstract class Backend implements TestPlanLifecycle {

  public void updateJobForTarget(Job job){
  }

}

