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


  public List<IntegrationEnv> allEnvironments() {

    Map<String, String> fromEnv = new HashMap<>();
    for (Map.Entry<String, String> entry : fromEnvironment.entrySet()) {
      var value = System.getenv(entry.getValue());
      if (value== null) {
        throw new RuntimeException("Environment variable " + entry.getValue() + " is undefined");
      }
      fromEnv.put(entry.getKey(), value);
    }

    // TODO: handle more matrix
    var modelList = matrix.get("MODEL");

    List<Map<String, String>> allMatrix = new ArrayList<>();
    modelList.forEach(
        model -> {
          var env = new HashMap<String, String>();
          env.put("MODEL", model);
          allMatrix.add(env);
        });

    List<IntegrationEnv> envs = new ArrayList<>();
    allMatrix.forEach(
        matrix -> {
          matrix.putAll(variables);
          matrix.putAll(fromEnv);

          LOGGER.info("XXX ENV BEFORE SUBS {}", matrix);
          var subs = new StringSubstitutor(matrix).setEnableUndefinedVariableException(true);
          matrix.forEach((key, value) -> {
            matrix.put(key, subs.replace(value));
          });

          LOGGER.info("XXX ENV AFTER SUBS {}", matrix);
          var env = new IntegrationEnv(matrix);
          envs.add(env);
        });
    return envs;
  }
}
