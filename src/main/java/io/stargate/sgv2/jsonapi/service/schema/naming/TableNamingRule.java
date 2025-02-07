package io.stargate.sgv2.jsonapi.service.schema.naming;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

/** The naming rule of the new Table name. */
public class TableNamingRule extends SchemaObjectNamingRule {
  public TableNamingRule() {
    super(SchemaObject.SchemaObjectType.TABLE);
  }
}
