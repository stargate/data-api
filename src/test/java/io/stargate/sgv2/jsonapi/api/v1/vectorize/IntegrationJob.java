package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import io.stargate.sgv2.jsonapi.api.model.command.CommandErrorFactory;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public record IntegrationJob(
    ITMetadata meta,
    Map<String, String> fromEnvironment,
    Map<String, String> variables,
    Map<String, List<String>> matrix,
    List<String> tests) {
  private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationJob.class);


  public IntegrationEnv withoutMatrix(){
    var fromEnv = new IntegrationEnv();
    for (Map.Entry<String, String> entry : fromEnvironment.entrySet()) {
      var value = System.getenv(entry.getValue());
      if (value== null) {
        throw new RuntimeException("Environment variable " + entry.getValue() + " is undefined");
      }
      fromEnv.put(entry.getKey(), value);
    }

    var fromVariables = new IntegrationEnv(variables);

    return fromEnv.clone().put(fromVariables);
  }
  public List<IntegrationEnv> allEnvironments() {

    var fromEnv = new IntegrationEnv();
    for (Map.Entry<String, String> entry : fromEnvironment.entrySet()) {
      var value = System.getenv(entry.getValue());
      if (value== null) {
        throw new RuntimeException("Environment variable " + entry.getValue() + " is undefined");
      }
      fromEnv.put(entry.getKey(), value);
    }

    var fromVariables = new IntegrationEnv(variables);

    // TODO: handle more matrix
    List<IntegrationEnv> fromMatrix = new ArrayList<>();
    matrix.get("MODEL").forEach(
        model -> {
          var env = new IntegrationEnv();
          env.put("MODEL", model);
          fromMatrix.add(env);
        });

    List<IntegrationEnv> allEnvs = new ArrayList<>();
    for (var matrixEnv : fromMatrix) {

      var completeEnv = fromEnv
          .clone()
          .put(fromVariables)
          .put(matrixEnv);
      allEnvs.add(completeEnv);
    }

    return allEnvs;
  }
}
