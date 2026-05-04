package io.stargate.sgv2.jsonapi.testbench.targets;

import io.stargate.sgv2.jsonapi.testbench.lifecycle.TestPlanLifecycle;
import io.stargate.sgv2.jsonapi.testbench.messaging.APIRequest;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestNodeFactory;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestRunEnv;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestUri;
import io.stargate.sgv2.jsonapi.testbench.testspec.Job;
import io.stargate.sgv2.jsonapi.testbench.testspec.TargetConfiguration;
import io.stargate.sgv2.jsonapi.testbench.testspec.TestCommand;
import io.stargate.sgv2.jsonapi.testbench.testspec.TestSuiteSpec;
import io.stargate.sgv2.jsonapi.testbench.testspec.WorkflowSpec;
import java.util.HashMap;
import java.util.Optional;
import org.junit.jupiter.api.DynamicNode;

public class Target implements TestPlanLifecycle {

  private final TargetConfiguration targetConfiguration;
  private final Backend backend;
  private final TestRunEnv env;

  public Target(TargetConfiguration targetConfiguration) {
    this.targetConfiguration = targetConfiguration;
    this.env = new TestRunEnv(new HashMap<>());

    this.backend =
        switch (targetConfiguration.backend()) {
          case "cassandra" -> new CassandraBackend();
          case "astra" -> new AstraBackend();
          default ->
              throw new IllegalArgumentException(
                  "Unknown backend: " + targetConfiguration.backend());
        };
  }

  public TargetConfiguration configuration() {
    return targetConfiguration;
  }

  public Connection connection() {
    return targetConfiguration.connection();
  }

  public void updateJobForTarget(Job job) {
    backend.updateJobForTarget(job);
  }

  public APIRequest apiRequest(TestCommand testCommand, TestRunEnv env) {
    return new APIRequest(targetConfiguration.connection(), env, testCommand.withEnvironment(env));
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
  public Optional<DynamicNode> beforeJob(TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, Job job) {
    return backend.beforeJob(testNodeFactory, uriBuilder, job);
  }

  @Override
  public Optional<DynamicNode> afterJob(TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, Job job) {
    return backend.afterJob(testNodeFactory, uriBuilder, job);
  }

  @Override
  public Optional<DynamicNode> beforeTestSuite(
          TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, TestSuiteSpec test, TestRunEnv env) {
    return backend.beforeTestSuite(testNodeFactory, uriBuilder, test, env);
  }

  @Override
  public Optional<DynamicNode> afterTestSuite(
          TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, TestSuiteSpec test, TestRunEnv env) {
    return backend.afterTestSuite(testNodeFactory, uriBuilder, test, env);
  }
}
