package io.stargate.sgv2.jsonapi.util.naming;

public class KeyspaceNamingRule extends SchemaObjectNamingRule {
  @Override
  public String name() {
    return "keyspace";
  }
}
