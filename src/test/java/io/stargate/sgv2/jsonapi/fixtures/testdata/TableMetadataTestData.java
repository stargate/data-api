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

  public TableMetadata tableAllDatatypesIndexed() {
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
            // supported datatype columns
            Map.entry(names.CQL_TEXT_COLUMN, DataTypes.TEXT),
            Map.entry(names.CQL_ASCII_COLUMN, DataTypes.ASCII),
            Map.entry(names.CQL_BLOB_COLUMN, DataTypes.BLOB),
            Map.entry(names.CQL_DURATION_COLUMN, DataTypes.DURATION),
            Map.entry(names.CQL_BOOLEAN_COLUMN, DataTypes.BOOLEAN),
            Map.entry(names.CQL_DECIMAL_COLUMN, DataTypes.DECIMAL),
            Map.entry(names.CQL_DOUBLE_COLUMN, DataTypes.DOUBLE),
            Map.entry(names.CQL_FLOAT_COLUMN, DataTypes.FLOAT),
            Map.entry(names.CQL_SMALLINT_COLUMN, DataTypes.SMALLINT),
            Map.entry(names.CQL_TINYINT_COLUMN, DataTypes.TINYINT),
            Map.entry(names.CQL_INT_COLUMN, DataTypes.INT),
            Map.entry(names.CQL_BIGINT_COLUMN, DataTypes.BIGINT),
            Map.entry(names.CQL_VARINT_COLUMN, DataTypes.VARINT),
            Map.entry(names.CQL_DATE_COLUMN, DataTypes.DATE),
            Map.entry(names.CQL_TIMESTAMP_COLUMN, DataTypes.TIMESTAMP),
            Map.entry(names.CQL_TIME_COLUMN, DataTypes.TIME)),
        ImmutableMap.of(),
        ImmutableMap.ofEntries(
            // index all datatypes column in the table,(Blob,Duration can NOT be indexed)
            Map.entry(
                names.CQL_TEXT_COLUMN_INDEX,
                indexMetadata(names.CQL_TEXT_COLUMN_INDEX, names.CQL_TEXT_COLUMN)),
            Map.entry(
                names.CQL_ASCII_COLUMN_INDEX,
                indexMetadata(names.CQL_ASCII_COLUMN_INDEX, names.CQL_ASCII_COLUMN)),
            Map.entry(
                names.CQL_BOOLEAN_COLUMN_INDEX,
                indexMetadata(names.CQL_BOOLEAN_COLUMN_INDEX, names.CQL_BOOLEAN_COLUMN)),
            Map.entry(
                names.CQL_DECIMAL_COLUMN_INDEX,
                indexMetadata(names.CQL_DECIMAL_COLUMN_INDEX, names.CQL_DECIMAL_COLUMN)),
            Map.entry(
                names.CQL_DOUBLE_COLUMN_INDEX,
                indexMetadata(names.CQL_DOUBLE_COLUMN_INDEX, names.CQL_DOUBLE_COLUMN)),
            Map.entry(
                names.CQL_FLOAT_COLUMN_INDEX,
                indexMetadata(names.CQL_FLOAT_COLUMN_INDEX, names.CQL_FLOAT_COLUMN)),
            Map.entry(
                names.CQL_SMALLINT_COLUMN_INDEX,
                indexMetadata(names.CQL_SMALLINT_COLUMN_INDEX, names.CQL_SMALLINT_COLUMN)),
            Map.entry(
                names.CQL_TINYINT_COLUMN_INDEX,
                indexMetadata(names.CQL_TINYINT_COLUMN_INDEX, names.CQL_TINYINT_COLUMN)),
            Map.entry(
                names.CQL_INT_COLUMN_INDEX,
                indexMetadata(names.CQL_INT_COLUMN_INDEX, names.CQL_INT_COLUMN)),
            Map.entry(
                names.CQL_BIGINT_COLUMN_INDEX,
                indexMetadata(names.CQL_BIGINT_COLUMN_INDEX, names.CQL_BIGINT_COLUMN)),
            Map.entry(
                names.CQL_VARINT_COLUMN_INDEX,
                indexMetadata(names.CQL_VARINT_COLUMN_INDEX, names.CQL_VARINT_COLUMN)),
            Map.entry(
                names.CQL_DATE_COLUMN_INDEX,
                indexMetadata(names.CQL_DATE_COLUMN_INDEX, names.CQL_DATE_COLUMN)),
            Map.entry(
                names.CQL_TIMESTAMP_COLUMN_INDEX,
                indexMetadata(names.CQL_TIMESTAMP_COLUMN_INDEX, names.CQL_TIMESTAMP_COLUMN)),
            Map.entry(
                names.CQL_TIME_COLUMN_INDEX,
                indexMetadata(names.CQL_TIME_COLUMN_INDEX, names.CQL_TIME_COLUMN))));
  }

  public TableMetadata tableAllDatatypesNotIndexed() {
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
            // supported datatype columns
            Map.entry(names.CQL_TEXT_COLUMN, DataTypes.TEXT),
            Map.entry(names.CQL_ASCII_COLUMN, DataTypes.ASCII),
            Map.entry(names.CQL_BLOB_COLUMN, DataTypes.BLOB),
            Map.entry(names.CQL_DURATION_COLUMN, DataTypes.DURATION),
            Map.entry(names.CQL_BOOLEAN_COLUMN, DataTypes.BOOLEAN),
            Map.entry(names.CQL_DECIMAL_COLUMN, DataTypes.DECIMAL),
            Map.entry(names.CQL_DOUBLE_COLUMN, DataTypes.DOUBLE),
            Map.entry(names.CQL_FLOAT_COLUMN, DataTypes.FLOAT),
            Map.entry(names.CQL_SMALLINT_COLUMN, DataTypes.SMALLINT),
            Map.entry(names.CQL_TINYINT_COLUMN, DataTypes.TINYINT),
            Map.entry(names.CQL_INT_COLUMN, DataTypes.INT),
            Map.entry(names.CQL_BIGINT_COLUMN, DataTypes.BIGINT),
            Map.entry(names.CQL_VARINT_COLUMN, DataTypes.VARINT),
            Map.entry(names.CQL_DATE_COLUMN, DataTypes.DATE),
            Map.entry(names.CQL_TIMESTAMP_COLUMN, DataTypes.TIMESTAMP),
            Map.entry(names.CQL_TIME_COLUMN, DataTypes.TIME)),
        ImmutableMap.of(),
        ImmutableMap.of());
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
