package io.stargate.sgv2.jsonapi.service.schema.naming;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

/** The naming rule of the new Keyspace name. */
public class KeyspaceNamingRule extends SchemaObjectNamingRule {
  public KeyspaceNamingRule() {
    super(SchemaObject.SchemaObjectType.KEYSPACE);
  }
}
