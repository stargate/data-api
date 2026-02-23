package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.TestAssertion;
import org.apache.commons.lang3.RandomStringUtils;

import static io.stargate.sgv2.jsonapi.api.v1.vectorize.TestEnvironment.toSafeSchemaIdentifier;

public class CassandraBackend extends Backend {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public void updateJobForTarget(Job job) {
    var keyspaceName = toSafeSchemaIdentifier(
        "job_" + job.meta().name().substring(0, Math.min(job.meta().name().length(), 27)) + "_" + RandomStringUtils.insecure().nextAlphanumeric(16));
    job.variables().put("KEYSPACE_NAME", keyspaceName);
  }

  @Override
  public void jobStarting(TestPlan testPlan, Job job) {
    // max length for keyspace is 48 chars
    var command = TestCommand.fromJson(
        """
            {
              "createKeyspace": {
                "name": "${KEYSPACE_NAME}"
              }
            }
            """);

    var setupRequest = new TestRequest(command, testPlan.target(), job.withoutMatrix(testPlan), TestAssertion.forSuccess(command.commandName()));

    var setupResponse = setupRequest.execute();
    setupResponse.validate(null, null, true);

  }

  @Override
  public void jobFinished(TestPlan testPlan, Job job) {
    var command = TestCommand.fromJson(
        """
           {
            "dropKeyspace": {
              "name": "${KEYSPACE_NAME}"
              }
            }
            """);

    var setupRequest = new TestRequest(command, testPlan.target(), job.withoutMatrix(testPlan), TestAssertion.forSuccess(command.commandName()));

    var setupResponse = setupRequest.execute();
    setupResponse.validate(null, null, true);
  }
}
