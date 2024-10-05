package io.stargate.sgv2.jsonapi.fixtures.testdata;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultColumnMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultTableMetadata;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

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
        List.of(),
        Map.of(),
        Map.of(),
        Map.of(),
        Map.of());
  }

  public TableMetadata keyValue() {
    return new DefaultTableMetadata(
        names.KEYSPACE_NAME,
        names.TABLE_NAME,
        UUID.randomUUID(),
        false,
        false,
        List.of(columnMetadata(names.COL_PARTITION_KEY_1, DataTypes.TEXT)),
        Map.of(),
        columnMap(
            Map.entry(names.COL_PARTITION_KEY_1, DataTypes.TEXT),
            Map.entry(names.COL_REGULAR_KEY_1, DataTypes.TEXT)),
        Map.of(),
        Map.of());
  }

  public TableMetadata TableWith2PartitionKeys3ClusteringKeys() {
    return new DefaultTableMetadata(
        names.KEYSPACE_NAME,
        names.TABLE_NAME,
        UUID.randomUUID(),
        false,
        false,
        List.of(
            columnMetadata(names.COL_PARTITION_KEY_1, DataTypes.TEXT),
            columnMetadata(names.COL_PARTITION_KEY_2, DataTypes.TEXT)),
        Map.of(
            columnMetadata(names.COL_CLUSTERING_KEY_1, DataTypes.TEXT),
            ClusteringOrder.ASC,
            columnMetadata(names.COL_CLUSTERING_KEY_2, DataTypes.TEXT),
            ClusteringOrder.ASC,
            columnMetadata(names.COL_CLUSTERING_KEY_3, DataTypes.TEXT),
            ClusteringOrder.ASC),
        Map.of(
            names.COL_PARTITION_KEY_1, columnMetadata(names.COL_PARTITION_KEY_1, DataTypes.TEXT),
            names.COL_PARTITION_KEY_2, columnMetadata(names.COL_PARTITION_KEY_2, DataTypes.TEXT),
            names.COL_CLUSTERING_KEY_1, columnMetadata(names.COL_CLUSTERING_KEY_1, DataTypes.TEXT),
            names.COL_CLUSTERING_KEY_2, columnMetadata(names.COL_CLUSTERING_KEY_2, DataTypes.TEXT),
            names.COL_CLUSTERING_KEY_3, columnMetadata(names.COL_CLUSTERING_KEY_3, DataTypes.TEXT),
            names.COL_REGULAR_KEY_1, columnMetadata(names.COL_REGULAR_KEY_1, DataTypes.TEXT),
            names.COL_REGULAR_KEY_2, columnMetadata(names.COL_REGULAR_KEY_2, DataTypes.TEXT)),
        Map.of(),
        Map.of());
  }

  public ColumnMetadata columnMetadata(CqlIdentifier columnName, DataType dataType) {
    return new DefaultColumnMetadata(
        names.KEYSPACE_NAME, names.TABLE_NAME, columnName, dataType, false);
  }

  public Map<CqlIdentifier, ColumnMetadata> columnMap(
      Map.Entry<CqlIdentifier, DataType>... nameType) {
    return Arrays.stream(nameType)
        .collect(
            Collectors.toMap(Map.Entry::getKey, e -> columnMetadata(e.getKey(), e.getValue())));
  }
}
