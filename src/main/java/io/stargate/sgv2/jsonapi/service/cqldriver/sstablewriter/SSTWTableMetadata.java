package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class SSTWTableMetadata implements TableMetadata {
  private final String tableName;
  private final Map<CqlIdentifier, ColumnMetadata> columns;
  private final List<ColumnMetadata> partitionKey;
  private final Map<ColumnMetadata, ClusteringOrder> clusteringColumns;
  private final UUID id;

  public SSTWTableMetadata(
      String tableName,
      Map<CqlIdentifier, ColumnMetadata> columns,
      List<ColumnMetadata> partitionKey,
      Map<ColumnMetadata, ClusteringOrder> clusteringColumns) {
    this.tableName = tableName;
    this.columns = columns;
    this.partitionKey = partitionKey;
    this.clusteringColumns = clusteringColumns;
    this.id = UUID.randomUUID();
  }

  @Override
  public boolean isCompactStorage() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isVirtual() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @NonNull
  @Override
  public Map<CqlIdentifier, IndexMetadata> getIndexes() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @NonNull
  @Override
  public CqlIdentifier getKeyspace() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @NonNull
  @Override
  public CqlIdentifier getName() {
    return CqlIdentifier.fromCql(tableName);
  }

  @Override
  public Optional<UUID> getId() {
    return Optional.of(id);
  }

  @NonNull
  @Override
  public List<ColumnMetadata> getPartitionKey() {
    return this.partitionKey;
  }

  @NonNull
  @Override
  public Map<ColumnMetadata, ClusteringOrder> getClusteringColumns() {
    return this.clusteringColumns;
  }

  @NonNull
  @Override
  public Map<CqlIdentifier, ColumnMetadata> getColumns() {
    return this.columns;
  }

  @NonNull
  @Override
  public Map<CqlIdentifier, Object> getOptions() {
    throw new UnsupportedOperationException("Not implemented");
  }
}
