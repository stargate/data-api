package io.stargate.sgv2.jsonapi.service.schema;

import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.IndexUsage;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;

/**
 * A Database in the API.
 * <p>
 * While we don't do much with a database itself in the API, we do some commands like
 * listing keyspaces that conceptually belong to the database. So we have a schema
 * object for it.
 * <p>
 * Currently only identified by the tenant.
 */
public class DatabaseSchemaObject extends SchemaObject {

  public DatabaseSchemaObject(Tenant tenant) {
    super(SchemaObjectType.DATABASE, SchemaObjectIdentifier.forDatabase(tenant));
  }

  public DatabaseSchemaObject(SchemaObjectIdentifier identifier) {
    super(SchemaObjectType.DATABASE, identifier);
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
