package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.backends.AstraBackend;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.backends.Backend;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.backends.CassandraBackend;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.lifecycle.TestPlanLifecycle;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TestSuite;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TestUri;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.Workflow;
import org.junit.jupiter.api.DynamicNode;

import java.util.HashMap;
import java.util.Optional;

public class Target implements TestPlanLifecycle {

  private final TargetConfiguration targetConfiguration;
  private final Backend backend;
  private final TestEnvironment env;

  public Target(TargetConfiguration targetConfiguration) {
    this.targetConfiguration = targetConfiguration;
    this.env = new TestEnvironment(new HashMap<>());

    this.backend = switch (targetConfiguration.backend()) {
      case "cassandra" -> new CassandraBackend();
      case "astra" -> new AstraBackend();
      default -> throw new IllegalArgumentException("Unknown backend: " + targetConfiguration.backend());
    };
  }

  public TargetConfiguration configuration(){
    return targetConfiguration;
  }
  public Connection connection(){
    return targetConfiguration.connection();
  }

  public void updateJobForTarget(Job job){
    backend.updateJobForTarget(job);
  }


  public APIRequest apiRequest(TestCommand testCommand, TestEnvironment env){
    return new APIRequest(targetConfiguration.connection(),  env, testCommand.withEnvironment(env));
  }

  @Override
  public Optional<DynamicNode> beforeWorkflow(TestPlan testPlan, TestUri.Builder uriBuilder, Workflow workflow){
    return backend.beforeWorkflow(testPlan, uriBuilder,workflow);
  }
  @Override
  public Optional<DynamicNode>  afterWorkflow(TestPlan testPlan, TestUri.Builder uriBuilder,Workflow workflow){
    return backend.afterWorkflow(testPlan, uriBuilder,workflow);
  }

  @Override
  public Optional<DynamicNode>  beforeJob(TestPlan testPlan, TestUri.Builder uriBuilder,Job job){
    return backend.beforeJob(testPlan, uriBuilder,job);
  }
  @Override
  public Optional<DynamicNode>  afterJob(TestPlan testPlan,TestUri.Builder uriBuilder,Job job){
    return backend.afterJob(testPlan,uriBuilder, job);
  }

  @Override
  public Optional<DynamicNode>  beforeTestSuite(TestPlan testPlan, TestUri.Builder uriBuilder,TestSuite test, TestEnvironment env){
    return backend.beforeTestSuite(testPlan,uriBuilder, test, env);
  }
  @Override
  public Optional<DynamicNode>  afterTestSuite(TestPlan testPlan, TestUri.Builder uriBuilder,TestSuite test, TestEnvironment env){
    return backend.afterTestSuite(testPlan, uriBuilder,test, env);
  }
}
