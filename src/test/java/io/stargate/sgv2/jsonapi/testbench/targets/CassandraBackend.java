package io.stargate.sgv2.jsonapi.testbench.targets;

import io.stargate.sgv2.jsonapi.testbench.assertions.TestAssertion;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestExecutionCondition;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestNodeFactory;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestRunRequest;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestUri;
import io.stargate.sgv2.jsonapi.testbench.testspec.Job;
import io.stargate.sgv2.jsonapi.testbench.testspec.TestCommand;
import java.util.Optional;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.DynamicNode;

/** A classic Cassandra backend, running locally or elsewhere. */
public class CassandraBackend extends Backend {

  public static final String NAME = "cassandra";

  @Override
  public void updateJobForTarget(Job job) {

    // We are going to create the keyspace for every job, so making a new name here based on the
    // name
    // of the job, and some random because the name will be trunc'd
    var keyspaceName =
        toSafeSchemaIdentifier(
            "job_"
                + job.meta().name().substring(0, Math.min(job.meta().name().length(), 27))
                + "_"
                + RandomStringUtils.insecure().nextAlphanumeric(16));
    job.variables().put("KEYSPACE_NAME", keyspaceName);
  }

  /** Need to create a keyspace, because C* does not have a default one. */
  @Override
  public Optional<DynamicNode> beforeJob(
      TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, Job job) {

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

  /** Drop the keyspace we made for this job. */
  @Override
  public Optional<DynamicNode> afterJob(
      TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, Job job) {
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
