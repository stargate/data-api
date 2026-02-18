package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import java.util.HashMap;

public class IntegrationTarget {

  private final Target target;
  private final Backend  backend;
  private final IntegrationEnv env;

  public IntegrationTarget(Target target) {
    this.target = target;
    this.env = new IntegrationEnv(new HashMap<>());

    this.backend = switch (target.backend()) {
      case "cassandra" -> new CassandraBackend();
      case "astra" -> new AstraBackend();
      default -> throw new IllegalArgumentException("Unknown backend: " + target.backend());
    };
  }

  public Connection connection(){
    return target.connection();
  }

  public APIRequest apiRequest(TestRequest testRequest, IntegrationEnv  env){
    return new APIRequest(target.connection(), testRequest, env);
  }

  public void workflowStarting(IntegrationWorkflow workflow){ }
  public void workflowFinished(IntegrationWorkflow workflow){ }

  public void jobStarting(IntegrationJob job){
    backend.jobStarting(this, job);
  }
  public void jobFinished(IntegrationJob job){
    backend.jobFinished(this, job);
  }

  public void testStarting(IntegrationTest test, IntegrationEnv env){ }
  public void testFinished(IntegrationTest test,  IntegrationEnv env){ }
}
