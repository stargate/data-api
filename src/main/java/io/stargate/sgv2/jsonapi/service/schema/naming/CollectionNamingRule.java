package io.stargate.sgv2.jsonapi.service.schema.naming;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

/** The naming rule of the new Collection name. */
public class CollectionNamingRule extends SchemaObjectNamingRule {
  public CollectionNamingRule() {
    super(SchemaObject.SchemaObjectType.COLLECTION);
  }
}
