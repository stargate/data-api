package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public record IntegrationEnv(Map<String, String> vars) {
  private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationEnv.class);

  private static final List<String> SCHEMA_IDENTIFIER = List.of("COLLECTION_NAME");

  public IntegrationEnv {

    for (var name : SCHEMA_IDENTIFIER) {
      if (vars.containsKey(name)) {
        var oldValue = vars.get(name);
        var newValue = oldValue.replaceAll("[^A-Za-z0-9_]", "_");

        if (newValue.length() > 48){
          throw new RuntimeException("Schema Identifier longer than 48 characters %s=%s".formatted(name,newValue));
        }
        LOGGER.info("XXX Updated IntegrationEnv value because it is a Schema Identifier key={}, oldValue={}, newValue={}",name,oldValue,newValue);
        vars.put(name, newValue);
      }
    }
  }
  public String requiredValue(String name){
    if (vars.containsKey(name)){
      return vars.get(name);
    }
    throw new RuntimeException(String.format("Required parameter %s not found", name));
  }
}
