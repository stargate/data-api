package io.stargate.sgv2.jsonapi.api.v1.util;

/** Helper class used for constructing and sending commands to the Data API */
public class DataApiCommandSenders {

  public static DataApiGeneralCommandSender assertGeneralCommand() {
    return new DataApiGeneralCommandSender();
  }

  public static DataApiKeyspaceCommandSender assertNamespaceCommand(String keyspace) {
    return new DataApiKeyspaceCommandSender(keyspace);
  }

  public static DataApiTableCommandSender assertTableCommand(String keyspace, String tableName) {
    return new DataApiTableCommandSender(keyspace, tableName);
  }
}
