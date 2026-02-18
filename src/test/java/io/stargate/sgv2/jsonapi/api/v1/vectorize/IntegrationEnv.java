package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.lookup.StringLookup;
import org.apache.commons.text.lookup.StringLookupFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class IntegrationEnv{

  private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationEnv.class);
  private static final Pattern PATTERN_NOT_WORD_CHARS = Pattern.compile("\\W+");

  private static final Set<String> SCHEMA_IDENTIFIER = Set.of("KEYSPACE_NAME", "COLLECTION_NAME");

  private final Map<String, String> vars = new HashMap<>();

  public IntegrationEnv(){
    this(new HashMap<>());
  }

  public IntegrationEnv(Map<String, String> vars) {
    this.vars.putAll(vars);
  }

  private IntegrationEnv(IntegrationEnv other){
    this.vars.putAll(other.vars);
  }

  public IntegrationEnv clone(){
    return new IntegrationEnv(this);
  }

  public IntegrationEnv put(IntegrationEnv other){
    this.vars.putAll(other.vars);
    return this;
  }

  public void put(String key, String value){
    this.vars.put(key, value);
  }

  public String requiredValue(String name){
    if (vars.containsKey(name)){
      return get(name);
    }
    throw new RuntimeException(String.format("Required env var %s not found", name));
  }

  public StringSubstitutor substitutor(){

    return new StringSubstitutor(StringLookupFactory.INSTANCE.functionStringLookup(this::get)).setEnableUndefinedVariableException(true);
  }
  private String get(String name){

    var value = vars.get(name);
    if (value == null){
      return "";
    }

    var substituted = substitutor().replace(value);
    var cleaned = SCHEMA_IDENTIFIER.contains(name) ?
      toSafeSchemaIdentifier(substituted)
        :
        substituted;

    return cleaned;
  }


  public static String toSafeSchemaIdentifier(String name){

    var newValue = PATTERN_NOT_WORD_CHARS.matcher(name).replaceAll("_");
    if (newValue.length() > 48){
      throw new RuntimeException("Schema Identifier longer than 48 characters %s=%s".formatted(name,newValue));
    }
    return newValue;
  }

  @Override
  public String toString() {
    return vars.toString();
  }
}
