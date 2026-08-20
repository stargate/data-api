package io.stargate.sgv2.jsonapi.testbench.targets;

import io.stargate.sgv2.jsonapi.testbench.testspec.Job;

/** DataStax / IBM Astra */
public class AstraBackend extends Backend {

  public static final String NAME = "astra";

  @Override
  public void updateJobForTarget(Job job) {

    // always using the `default_keyspace` for astra, because they cannot be made using the API
    job.variables().put("KEYSPACE_NAME", "default_keyspace");
  }
}
