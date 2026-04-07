package io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec;

public abstract class TestEnvAccess {

  public static void putEnvVar(String key, String value) {
    System.getProperties().put(key, value);
  }

  public static String getEnvVar(String varName) {

    var value = System.getProperty(varName);
    if (value != null){
      return value;
    }
    value = System.getenv(varName);
    if (value== null) {
      throw new RuntimeException("Environment variable not found in System Properties or Environment. varName=" + varName);
    }
    return value;
  }
}
