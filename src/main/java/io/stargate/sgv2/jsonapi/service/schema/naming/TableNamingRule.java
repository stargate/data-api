package io.stargate.sgv2.jsonapi.service.schema.naming;

import io.stargate.sgv2.jsonapi.service.schema.SchemaObjectType;

/** The naming rule of the new Table name. */
public class TableNamingRule extends SchemaObjectNamingRule {
  public TableNamingRule() {
    super(SchemaObjectType.TABLE);
  }
}
