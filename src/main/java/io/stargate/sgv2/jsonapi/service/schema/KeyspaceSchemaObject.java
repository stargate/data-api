package io.stargate.sgv2.jsonapi.service.schema;

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.IndexUsage;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import java.util.Objects;

/**
 * A Keyspace in the API.
 *
 * <p>We currently do not hang things like Tables and Views of the KeyspaceSchemaObject, they can
 * all be retrieved from {@link SchemaObjectCache}
 *
 * <p>We hae commands like list collections, list tables, etc. that run against as Keyspace so we
 * have an object to represent that Keyspace.
 */
public class KeyspaceSchemaObject extends SchemaObject {

  private KeyspaceMetadata keyspaceMetadata;

  @VisibleForTesting
  public KeyspaceSchemaObject(SchemaObjectIdentifier identifier) {
    super(SchemaObjectType.KEYSPACE, identifier);

    // here for existing testing, where creating the KeyspaceMetadata was not common
    this.keyspaceMetadata = null;
  }

  public KeyspaceSchemaObject(Tenant tenant, KeyspaceMetadata keyspaceMetadata) {
    super(
        SchemaObjectType.KEYSPACE,
        SchemaObjectIdentifier.forKeyspace(tenant, keyspaceMetadata.getName()));

    // keyspaceMetadata will prob be checked for null above, but for sanity, we ensure it's not null
    // here
    this.keyspaceMetadata =
        Objects.requireNonNull(keyspaceMetadata, "keyspaceMetadata must not be null");
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
