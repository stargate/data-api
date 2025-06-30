package io.stargate.sgv2.jsonapi.service.schema.naming;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

public class UdtNamingRule extends SchemaObjectNamingRule {

  public UdtNamingRule() {
    super(SchemaObject.SchemaObjectType.UDT);
  }
}
