package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import java.util.HashMap;

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

  public void workflowStarting(TestPlan testPlan, Workflow workflow){ }
  public void workflowFinished(TestPlan testPlan,Workflow workflow){ }

  public void jobStarting(TestPlan testPlan,Job job){
    backend.jobStarting(testPlan, job);
  }
  public void jobFinished(TestPlan testPlan,Job job){
    backend.jobFinished(testPlan, job);
  }

  public void testRunStarting(TestPlan testPlan,TestSuite test, TestEnvironment env){ }
  public void testRunFinished(TestPlan testPlan,TestSuite test, TestEnvironment env){ }
}
