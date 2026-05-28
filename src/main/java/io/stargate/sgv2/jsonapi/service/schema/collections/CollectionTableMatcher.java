package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.util.ColumnMetadataMatcher;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/** Simple class that can check if table is a matching jsonapi table. */
public class CollectionTableMatcher implements Predicate<TableMetadata> {

  private final Predicate<ColumnMetadata> primaryKeyPredicate;

  private final Predicate<ColumnMetadata> columnsPredicate;

  private final Predicate<ColumnMetadata> columnsPredicateVector;

  public CollectionTableMatcher() {
    primaryKeyPredicate =
        new ColumnMetadataMatcher.Tuple(
            CqlIdentifier.fromInternal("key"),
            new PrimitiveType(ProtocolConstants.DataType.TINYINT),
            new PrimitiveType(ProtocolConstants.DataType.VARCHAR));
    columnsPredicate =
        new ColumnMetadataMatcher.BasicType(
                CqlIdentifier.fromInternal("tx_id"),
                new PrimitiveType(ProtocolConstants.DataType.TIMEUUID))
            .or(
                new ColumnMetadataMatcher.Tuple(
                    CqlIdentifier.fromInternal("key"),
                    new PrimitiveType(ProtocolConstants.DataType.TINYINT),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new ColumnMetadataMatcher.BasicType(
                    CqlIdentifier.fromInternal("doc_json"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new ColumnMetadataMatcher.Set(
                    CqlIdentifier.fromInternal("exist_keys"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new ColumnMetadataMatcher.Map(
                    CqlIdentifier.fromInternal("array_size"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                    new PrimitiveType(ProtocolConstants.DataType.INT)))
            .or(
                new ColumnMetadataMatcher.Set(
                    CqlIdentifier.fromInternal("array_contains"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new ColumnMetadataMatcher.Map(
                    CqlIdentifier.fromInternal("query_bool_values"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                    new PrimitiveType(ProtocolConstants.DataType.TINYINT)))
            .or(
                new ColumnMetadataMatcher.Map(
                    CqlIdentifier.fromInternal("query_dbl_values"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                    new PrimitiveType(ProtocolConstants.DataType.DECIMAL)))
            .or(
                new ColumnMetadataMatcher.Map(
                    CqlIdentifier.fromInternal(
                        DocumentConstants.Columns.QUERY_TEXT_MAP_COLUMN_NAME),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new ColumnMetadataMatcher.Map(
                    CqlIdentifier.fromInternal("query_timestamp_values"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                    new PrimitiveType(ProtocolConstants.DataType.TIMESTAMP)))
            .or(
                new ColumnMetadataMatcher.Set(
                    CqlIdentifier.fromInternal("query_null_values"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new ColumnMetadataMatcher.BasicType(
                    CqlIdentifier.fromInternal(DocumentConstants.Columns.LEXICAL_INDEX_COLUMN_NAME),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)));

    // TODO: do not duplicate all of the code above below here, just add one extra predicate if we
    // need to test for a vector.
    columnsPredicateVector =
        new ColumnMetadataMatcher.BasicType(
                CqlIdentifier.fromInternal("tx_id"),
                new PrimitiveType(ProtocolConstants.DataType.TIMEUUID))
            .or(
                new ColumnMetadataMatcher.Tuple(
                    CqlIdentifier.fromInternal("key"),
                    new PrimitiveType(ProtocolConstants.DataType.TINYINT),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new ColumnMetadataMatcher.BasicType(
                    CqlIdentifier.fromInternal("doc_json"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new ColumnMetadataMatcher.Set(
                    CqlIdentifier.fromInternal("exist_keys"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new ColumnMetadataMatcher.Map(
                    CqlIdentifier.fromInternal("array_size"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                    new PrimitiveType(ProtocolConstants.DataType.INT)))
            .or(
                new ColumnMetadataMatcher.Set(
                    CqlIdentifier.fromInternal("array_contains"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new ColumnMetadataMatcher.Map(
                    CqlIdentifier.fromInternal("query_bool_values"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                    new PrimitiveType(ProtocolConstants.DataType.TINYINT)))
            .or(
                new ColumnMetadataMatcher.Map(
                    CqlIdentifier.fromInternal("query_dbl_values"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                    new PrimitiveType(ProtocolConstants.DataType.DECIMAL)))
            .or(
                new ColumnMetadataMatcher.Map(
                    CqlIdentifier.fromInternal(
                        DocumentConstants.Columns.QUERY_TEXT_MAP_COLUMN_NAME),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new ColumnMetadataMatcher.Map(
                    CqlIdentifier.fromInternal("query_timestamp_values"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                    new PrimitiveType(ProtocolConstants.DataType.TIMESTAMP)))
            .or(
                new ColumnMetadataMatcher.Set(
                    CqlIdentifier.fromInternal("query_null_values"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new ColumnMetadataMatcher.BasicType(
                    CqlIdentifier.fromInternal(DocumentConstants.Columns.LEXICAL_INDEX_COLUMN_NAME),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new ColumnMetadataMatcher.Vector(
                    CqlIdentifier.fromInternal("query_vector_value"),
                    new PrimitiveType(ProtocolConstants.DataType.FLOAT)));
  }

  /**
   * Tests if the given table is a valid jsonapi table.
   *
   * @param cqlTable the table
   * @return Returns true only if all the columns in the table correspond to the data-api table
   *     schema.
   */
  @Override
  public boolean test(TableMetadata cqlTable) {
    // null safety
    if (null == cqlTable) {
      return false;
    }

    // partition columns
    List<ColumnMetadata> partitionColumns = cqlTable.getPartitionKey();
    if (partitionColumns.size() != 1 || !partitionColumns.stream().allMatch(primaryKeyPredicate)) {
      return false;
    }

    // clustering columns
    Map<ColumnMetadata, ClusteringOrder> clusteringColumns = cqlTable.getClusteringColumns();
    if (clusteringColumns.size() != 0) {
      return false;
    }

    Collection<ColumnMetadata> columns = cqlTable.getColumns().values();
    if (!(columns.stream().allMatch(columnsPredicate)
        || columns.stream().allMatch(columnsPredicateVector))) {
      return false;
    }

    return true;
  }
}
