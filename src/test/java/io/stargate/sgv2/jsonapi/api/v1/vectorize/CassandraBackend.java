package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RandomStringUtils;

import static io.stargate.sgv2.jsonapi.api.v1.vectorize.IntegrationEnv.toSafeSchemaIdentifier;

public class CassandraBackend extends Backend {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();


  @Override
  public void jobStarting(IntegrationTarget integrationTarget, IntegrationJob job) {
    // max length for keyspace is 48 chars

    var keyspaceName = toSafeSchemaIdentifier(
        "job_" + job.meta().name().substring(0, Math.min(job.meta().name().length(), 27)) + "_" + RandomStringUtils.insecure().nextAlphanumeric(16));

    var request = TestRequest.fromJson(
        """
            {
              "createKeyspace": {
                "name": "${KEYSPACE_NAME}"
              }
            }
            """);

    job.variables().put("KEYSPACE_NAME", keyspaceName);
    integrationTarget.apiRequest(request, job.withoutMatrix()).executeWithSuccess();

  }

  @Override
  public void jobFinished(IntegrationTarget integrationTarget, IntegrationJob job) {
    var request = TestRequest.fromJson(
        """
           {
            "dropKeyspace": {
              "name": "${KEYSPACE_NAME}"
              }
            }
            """);

    integrationTarget.apiRequest(request, job.withoutMatrix()).executeWithSuccess();
  }
}
