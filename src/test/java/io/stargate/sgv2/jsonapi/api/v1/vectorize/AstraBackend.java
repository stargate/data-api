package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import org.apache.commons.lang3.RandomStringUtils;

import static io.stargate.sgv2.jsonapi.api.v1.vectorize.TestEnvironment.toSafeSchemaIdentifier;

public class AstraBackend extends Backend {

  @Override
  public void updateJobForTarget(Job job) {

    job.variables().put("KEYSPACE_NAME", "default_keyspace");
  }
}
