package io.stargate.sgv2.jsonapi.api.v1.util;

/** Helper class used for constructing and sending commands to the Data API */
public class DataApiCommandSenders {
  public static DataApiNamespaceCommandSender assertNamespaceCommand(String namespace) {
    return new DataApiNamespaceCommandSender(namespace);
  }

  public static DataApiTableCommandSender assertTableCommand(String namespace, String tableName) {
    return new DataApiTableCommandSender(namespace, tableName);
  }
}
