package io.stargate.sgv2.jsonapi.service.schema.naming;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

/** The naming rule of the new Index name. */
public class IndexNamingRule extends SchemaObjectNamingRule {
  public IndexNamingRule() {
    super(SchemaObject.SchemaObjectType.INDEX);
  }
}
