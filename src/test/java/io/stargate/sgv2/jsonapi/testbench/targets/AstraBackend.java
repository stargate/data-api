package io.stargate.sgv2.jsonapi.testbench.targets;

import io.stargate.sgv2.jsonapi.testbench.testspec.Job;

public class AstraBackend extends Backend {

  @Override
  public void updateJobForTarget(Job job) {

    job.variables().put("KEYSPACE_NAME", "default_keyspace");
  }
}
