package io.stargate.sgv2.jsonapi.api.v1.vectorize.targets;

import static io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestRunEnv.toSafeSchemaIdentifier;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions.TestAssertion;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestExecutionCondition;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestNodeFactory;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestRunRequest;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestUri;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.Job;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TestCommand;
import java.util.Optional;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.DynamicNode;

/** Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh */
public class CassandraBackend extends Backend {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public void updateJobForTarget(Job job) {
    // max length for keyspace is 48 chars
    var keyspaceName =
        toSafeSchemaIdentifier(
            "job_"
                + job.meta().name().substring(0, Math.min(job.meta().name().length(), 27))
                + "_"
                + RandomStringUtils.insecure().nextAlphanumeric(16));
    job.variables().put("KEYSPACE_NAME", keyspaceName);
  }

  @Override
  public Optional<DynamicNode> beforeJob(TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, Job job) {

    var command =
        TestCommand.fromJson(
            """
            {
              "createKeyspace": {
                "name": "${KEYSPACE_NAME}"
              }
            }
            """);

    var env = job.withoutMatrix(testNodeFactory.testPlan());
    var setupRequest =
        new TestRunRequest(
            env.substitutor().replace("createKeyspace: ${KEYSPACE_NAME}"),
            command,
            testNodeFactory.testPlan().target(),
            env,
            TestAssertion.forSuccess(testNodeFactory.testPlan(), command),
            new TestExecutionCondition.AlwaysTrue("CassandraBackend.beforeJob()"));

    return Optional.of(setupRequest.testNodes(testNodeFactory, uriBuilder));
  }

  @Override
  public Optional<DynamicNode> afterJob(TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, Job job) {
    var command =
        TestCommand.fromJson(
            """
           {
            "dropKeyspace": {
              "name": "${KEYSPACE_NAME}"
              }
            }
            """);

    var env = job.withoutMatrix(testNodeFactory.testPlan());
    var setupRequest =
        new TestRunRequest(
            env.substitutor().replace("dropKeyspace: ${KEYSPACE_NAME}"),
            command,
                testNodeFactory.testPlan().target(),
            job.withoutMatrix(testNodeFactory.testPlan()),
            TestAssertion.forSuccess(testNodeFactory.testPlan(), command),
            new TestExecutionCondition.AlwaysTrue("CassandraBackend.afterJob()"));

    return Optional.of(setupRequest.testNodes(testNodeFactory, uriBuilder));
  }
}
