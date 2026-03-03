package io.stargate.sgv2.jsonapi.api.v1.vectorize.backends;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.Job;

public class AstraBackend extends Backend {

  @Override
  public void updateJobForTarget(Job job) {

    job.variables().put("KEYSPACE_NAME", "default_keyspace");
  }
}
