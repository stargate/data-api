package io.stargate.sgv2.jsonapi.fixtures.testdata;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.*;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultColumnMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultIndexMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultTableMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.UUID;

/**
 * <b>IMPORTANT:</b> Use the Guava ImmutableMaps and ImmutableLists like the TableParser in the
 * driver because these <b>PRESERVE ORDER</b>
 */
public class TableMetadataTestData extends TestDataSuplier {

  public TableMetadataTestData(TestData testData) {
    super(testData);
  }

  public TableMetadata empty() {
    return new DefaultTableMetadata(
        names.KEYSPACE_NAME,
        names.TABLE_NAME,
        UUID.randomUUID(),
        false,
        false,
        ImmutableList.of(),
        ImmutableMap.of(),
        ImmutableMap.of(),
        ImmutableMap.of(),
        ImmutableMap.of());
  }

  public TableMetadata keyValue() {
    return new DefaultTableMetadata(
        names.KEYSPACE_NAME,
        names.TABLE_NAME,
        UUID.randomUUID(),
        false,
        false,
        ImmutableList.of(columnMetadata(names.COL_PARTITION_KEY_1, DataTypes.TEXT)),
        ImmutableMap.of(),
        columnMap(
            Map.entry(names.COL_PARTITION_KEY_1, DataTypes.TEXT),
            Map.entry(names.COL_REGULAR_1, DataTypes.TEXT)),
        ImmutableMap.of(),
        ImmutableMap.of());
  }

  public TableMetadata keyAndTwoDuration() {
    return new DefaultTableMetadata(
        names.KEYSPACE_NAME,
        names.TABLE_NAME,
        UUID.randomUUID(),
        false,
        false,
        ImmutableList.of(columnMetadata(names.COL_PARTITION_KEY_1, DataTypes.TEXT)),
        ImmutableMap.of(),
        columnMap(
            Map.entry(names.COL_PARTITION_KEY_1, DataTypes.TEXT),
            Map.entry(names.COL_REGULAR_1, DataTypes.DURATION),
            Map.entry(names.COL_REGULAR_2, DataTypes.DURATION)),
        ImmutableMap.of(),
        ImmutableMap.of());
  }

  public TableMetadata table2PK3Clustering1Index() {
    return new DefaultTableMetadata(
        names.KEYSPACE_NAME,
        names.TABLE_NAME,
        UUID.randomUUID(),
        false,
        false,
        ImmutableList.of(
            columnMetadata(names.COL_PARTITION_KEY_1, DataTypes.TEXT),
            columnMetadata(names.COL_PARTITION_KEY_2, DataTypes.TEXT)),
        ImmutableMap.of(
            columnMetadata(names.COL_CLUSTERING_KEY_1, DataTypes.TEXT),
            ClusteringOrder.ASC,
            columnMetadata(names.COL_CLUSTERING_KEY_2, DataTypes.TEXT),
            ClusteringOrder.ASC,
            columnMetadata(names.COL_CLUSTERING_KEY_3, DataTypes.TEXT),
            ClusteringOrder.ASC),
        columnMap(
            Map.entry(names.COL_PARTITION_KEY_1, DataTypes.TEXT),
            Map.entry(names.COL_PARTITION_KEY_2, DataTypes.TEXT),
            Map.entry(names.COL_CLUSTERING_KEY_1, DataTypes.TEXT),
            Map.entry(names.COL_CLUSTERING_KEY_2, DataTypes.TEXT),
            Map.entry(names.COL_CLUSTERING_KEY_3, DataTypes.TEXT),
            Map.entry(names.COL_REGULAR_1, DataTypes.TEXT),
            Map.entry(names.COL_REGULAR_2, DataTypes.TEXT),
            Map.entry(names.COL_INDEXED_1, DataTypes.TEXT)),
        ImmutableMap.of(),
        ImmutableMap.of(
            names.INDEX_NAME_1, indexMetadata(names.INDEX_NAME_1, names.COL_INDEXED_1)));
  }

  public ColumnMetadata columnMetadata(CqlIdentifier columnName, DataType dataType) {
    return new DefaultColumnMetadata(
        names.KEYSPACE_NAME, names.TABLE_NAME, columnName, dataType, false);
  }

  public Map<CqlIdentifier, ColumnMetadata> columnMap(
      Map.Entry<CqlIdentifier, DataType>... nameType) {

    ImmutableMap.Builder<CqlIdentifier, ColumnMetadata> builder = ImmutableMap.builder();
    for (Map.Entry<CqlIdentifier, DataType> entry : nameType) {
      builder.put(entry.getKey(), columnMetadata(entry.getKey(), entry.getValue()));
    }
    return builder.build();
  }

  public IndexMetadata indexMetadata(CqlIdentifier indexName, CqlIdentifier targetColumn) {

    var options =
        ImmutableMap.of("class_name", "StorageAttachedIndex", "target", targetColumn.asInternal());

    return new DefaultIndexMetadata(
        names.KEYSPACE_NAME,
        names.TABLE_NAME,
        indexName,
        IndexKind.CUSTOM, // SAI are all these
        targetColumn.asInternal(),
        options);
  }
}
