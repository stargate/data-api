package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.AssertionMatcher;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.TestAssertion;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.DynamicNode;

import java.util.Optional;

import static io.stargate.sgv2.jsonapi.api.v1.vectorize.TestEnvironment.toSafeSchemaIdentifier;

/**
 * Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh
 */
public class CassandraBackend extends Backend {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public void updateJobForTarget(Job job) {
    // max length for keyspace is 48 chars
    var keyspaceName = toSafeSchemaIdentifier(
        "job_" + job.meta().name().substring(0, Math.min(job.meta().name().length(), 27)) + "_" + RandomStringUtils.insecure().nextAlphanumeric(16));
    job.variables().put("KEYSPACE_NAME", keyspaceName);
  }

  @Override
  public Optional<DynamicNode> beforeJob(TestPlan testPlan, Job job) {

    var command = TestCommand.fromJson(
        """
            {
              "createKeyspace": {
                "name": "${KEYSPACE_NAME}"
              }
            }
            """);

    var env = job.withoutMatrix(testPlan);
    var setupRequest = new TestRequest(
         env.substitutor().replace("createKeyspace: ${KEYSPACE_NAME}"),
        command, testPlan.target(), env, TestAssertion.forSuccess(command.commandName()));

    return Optional.of(setupRequest.testNodes());
  }

  @Override
  public Optional<DynamicNode> afterJob(TestPlan testPlan, Job job) {
    var command = TestCommand.fromJson(
        """
           {
            "dropKeyspace": {
              "name": "${KEYSPACE_NAME}"
              }
            }
            """);

    var env = job.withoutMatrix(testPlan);
    var setupRequest = new TestRequest(
        env.substitutor().replace("dropKeyspace: ${KEYSPACE_NAME}"),
        command, testPlan.target(), job.withoutMatrix(testPlan), TestAssertion.forSuccess(command.commandName()));

    return Optional.of(setupRequest.testNodes());
  }
}
