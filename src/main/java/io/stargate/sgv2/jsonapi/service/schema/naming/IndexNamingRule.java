package io.stargate.sgv2.jsonapi.service.schema.naming;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

/** The naming rule of the new Index name. */
public class IndexNamingRule extends SchemaObjectNamingRule {
  private static final int MAX_INDEX_NAME_LENGTH = 100;

  public IndexNamingRule() {
    super(SchemaObject.SchemaObjectType.INDEX);
  }

  @Override
  public int getMaxLength() {
    return MAX_INDEX_NAME_LENGTH;
  }
}
