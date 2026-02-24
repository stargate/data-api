package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import org.junit.jupiter.api.DynamicNode;

import java.util.HashMap;
import java.util.Optional;

public class Target {

  private final TargetConfiguration targetConfiguration;
  private final Backend  backend;
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

  public Optional<DynamicNode> beforeWorkflow(TestPlan testPlan, Workflow workflow){
    return backend.beforeWorkflow(testPlan, workflow);
  }
  public Optional<DynamicNode>  afterWorkflow(TestPlan testPlan,Workflow workflow){
    return backend.afterWorkflow(testPlan, workflow);
  }

  public Optional<DynamicNode>  beforeJob(TestPlan testPlan,Job job){
    return backend.beforeJob(testPlan, job);
  }
  public Optional<DynamicNode>  afterJob(TestPlan testPlan,Job job){
    return backend.afterJob(testPlan, job);
  }

  public Optional<DynamicNode>  beforeTestSuite(TestPlan testPlan,TestSuite test, TestEnvironment env){
    return backend.beforeTestSuite(testPlan, test, env);
  }
  public Optional<DynamicNode>  afterTestSuite(TestPlan testPlan,TestSuite test, TestEnvironment env){
    return backend.afterTestSuite(testPlan, test, env);
  }
}
