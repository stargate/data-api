package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObjectIdentifier;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObjectType;

public class KeyspaceSchemaObject extends SchemaObject {

  public KeyspaceSchemaObject(Tenant tenant, String keyspace) {
    this(SchemaObjectIdentifier.forKeyspace(tenant, keyspace));
  }

  public KeyspaceSchemaObject(SchemaObjectIdentifier identifier) {
    super(SchemaObjectType.KEYSPACE, identifier);
  }

  @Override
  public VectorConfig vectorConfig() {
    return VectorConfig.NOT_ENABLED_CONFIG;
  }

  @Override
  public IndexUsage newIndexUsage() {
    return IndexUsage.NO_OP;
  }
}
