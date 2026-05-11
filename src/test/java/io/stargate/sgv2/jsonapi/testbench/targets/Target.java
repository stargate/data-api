package io.stargate.sgv2.jsonapi.testbench.targets;

import io.stargate.sgv2.jsonapi.testbench.lifecycle.JobLifeCycle;
import io.stargate.sgv2.jsonapi.testbench.lifecycle.TestPlanLifecycle;
import io.stargate.sgv2.jsonapi.testbench.messaging.APIRequest;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestNodeFactory;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestRunEnv;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestUri;
import io.stargate.sgv2.jsonapi.testbench.testspec.Job;
import io.stargate.sgv2.jsonapi.testbench.testspec.TestCommand;
import io.stargate.sgv2.jsonapi.testbench.testspec.TestSuiteSpec;
import io.stargate.sgv2.jsonapi.testbench.testspec.WorkflowSpec;
import java.util.Optional;
import org.junit.jupiter.api.DynamicNode;

/**
 * A particular instance of a {@link Backend} we are going to run the test against, so includes
 * connection information etc.
 *
 * <p>A run of the Test Bench is run against a Target, e.g. cassandra on localhost, or an astra db
 * called monkeys.
 *
 * <p>This is the important entry point for the lifecycle interfaces, because the life cycle is
 * there to handle different target / backends that tests run against.
 *
 * <p>Because this holds the connection information, it can also make a request, see {@link
 * #apiRequest(TestCommand, TestRunEnv)}
 */
public class Target implements TestPlanLifecycle, JobLifeCycle {

  private final TargetConfiguration targetConfiguration;
  private final Backend backend;

  public Target(TargetConfiguration targetConfiguration) {
    this.targetConfiguration = targetConfiguration;

    this.backend =
        switch (targetConfiguration.backend()) {
          case CassandraBackend.NAME -> new CassandraBackend();
          case AstraBackend.NAME -> new AstraBackend();
          default ->
              throw new IllegalArgumentException(
                  "Unknown backend: " + targetConfiguration.backend());
        };
  }

  public TargetConfiguration configuration() {
    return targetConfiguration;
  }

  /**
   * Call this to get a new {@link APIRequest} that is configured to talk to the target this class
   * represents.
   *
   * @param testCommand The command the request will send, this is needed to get the actual DataAPI
   *     request we want to send.
   * @param env Environment the commands will be run in, used to make the replacements in the
   *     command to execute for this particular test run.
   * @return Configured {@link APIRequest}
   */
  public APIRequest apiRequest(TestCommand testCommand, TestRunEnv env) {
    return new APIRequest(targetConfiguration.connection(), env, testCommand.withEnvironment(env));
  }

  @Override
  public void updateJobForTarget(Job job) {
    backend.updateJobForTarget(job);
  }

  @Override
  public Optional<DynamicNode> beforeWorkflow(
      TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, WorkflowSpec workflow) {
    return backend.beforeWorkflow(testNodeFactory, uriBuilder, workflow);
  }

  @Override
  public Optional<DynamicNode> afterWorkflow(
      TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, WorkflowSpec workflow) {
    return backend.afterWorkflow(testNodeFactory, uriBuilder, workflow);
  }

  @Override
  public Optional<DynamicNode> beforeJob(
      TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, Job job) {
    return backend.beforeJob(testNodeFactory, uriBuilder, job);
  }

  @Override
  public Optional<DynamicNode> afterJob(
      TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, Job job) {
    return backend.afterJob(testNodeFactory, uriBuilder, job);
  }

  @Override
  public Optional<DynamicNode> beforeTestSuite(
      TestNodeFactory testNodeFactory,
      TestUri.Builder uriBuilder,
      TestSuiteSpec test,
      TestRunEnv env) {
    return backend.beforeTestSuite(testNodeFactory, uriBuilder, test, env);
  }

  @Override
  public Optional<DynamicNode> afterTestSuite(
      TestNodeFactory testNodeFactory,
      TestUri.Builder uriBuilder,
      TestSuiteSpec test,
      TestRunEnv env) {
    return backend.afterTestSuite(testNodeFactory, uriBuilder, test, env);
  }
}
