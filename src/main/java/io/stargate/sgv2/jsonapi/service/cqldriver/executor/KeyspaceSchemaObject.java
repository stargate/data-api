package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObjectIdentifier;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObjectType;

import java.util.Objects;

public class KeyspaceSchemaObject extends SchemaObject {

  private KeyspaceMetadata keyspaceMetadata;

  @VisibleForTesting
  public KeyspaceSchemaObject(SchemaObjectIdentifier identifier) {
    super(SchemaObjectType.KEYSPACE, identifier);

    this.keyspaceMetadata = null;
  }

  public KeyspaceSchemaObject(Tenant tenant, KeyspaceMetadata keyspaceMetadata) {
    super(SchemaObjectType.KEYSPACE, SchemaObjectIdentifier.forKeyspace(tenant, keyspaceMetadata.getName()));

    // keyspaceMetadata will prob be checked for null above, but for sanity, we ensure it's not null here
    this.keyspaceMetadata = Objects.requireNonNull(keyspaceMetadata, "keyspaceMetadata must not be null");
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
