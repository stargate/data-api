package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListenerBase;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaCache;
import java.util.Optional;

public class SchemaChangeListener extends SchemaChangeListenerBase {

  private final SchemaCache schemaCache;

  private final String tenantId;

  public SchemaChangeListener(SchemaCache schemaCache, String tenantId) {
    this.schemaCache = schemaCache;
    this.tenantId = tenantId;
  }

  /**
   * Add tableDropped event listener for every cqlSession Drop the corresponding collectionSetting
   * cache entry to avoid operations using outdated CollectionSetting This should work for both CQL
   * Table drop and Data API deleteCollection
   */
  public void onTableDropped(TableMetadata table) {
    schemaCache.evictCollectionSettingCacheEntry(
        Optional.ofNullable(tenantId),
        table.getKeyspace().asInternal(),
        table.getName().asInternal());
  }

  /**
   * Add keyspaceDropped event listener for every cqlSession Drop the corresponding namespaceCache
   * entry This should work for both CQL keyspace drop and Data API dropNamespace
   */
  @Override
  public void onKeyspaceDropped(@NonNull KeyspaceMetadata keyspace) {
    schemaCache.evictNamespaceCacheEntriesForTenant(tenantId, keyspace.getName().asInternal());
  }
}
