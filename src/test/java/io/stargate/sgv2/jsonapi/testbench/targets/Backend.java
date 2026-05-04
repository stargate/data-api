package io.stargate.sgv2.jsonapi.testbench.targets;

import io.stargate.sgv2.jsonapi.testbench.lifecycle.TestPlanLifecycle;
import io.stargate.sgv2.jsonapi.testbench.testspec.Job;

public abstract class Backend implements TestPlanLifecycle {

  public void updateJobForTarget(Job job) {}
}
