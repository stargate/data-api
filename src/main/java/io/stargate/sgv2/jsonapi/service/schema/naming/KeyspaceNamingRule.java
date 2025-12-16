package io.stargate.sgv2.jsonapi.service.schema.naming;


/** The naming rule of the new Keyspace name. */
public class KeyspaceNamingRule extends SchemaObjectNamingRule {
  public KeyspaceNamingRule() {
    super(SchemaObjectType.KEYSPACE);
  }
}
