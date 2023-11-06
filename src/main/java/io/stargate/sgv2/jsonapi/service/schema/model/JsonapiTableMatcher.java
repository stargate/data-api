package io.stargate.sgv2.jsonapi.service.schema.model;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/** Simple class that can check if table is a matching jsonapi table. */
public class JsonapiTableMatcher implements Predicate<TableMetadata> {

  private final Predicate<ColumnMetadata> primaryKeyPredicate;

  private final Predicate<ColumnMetadata> columnsPredicate;

  private final Predicate<ColumnMetadata> columnsPredicateVector;

  public JsonapiTableMatcher() {
    primaryKeyPredicate =
        new CqlColumnMatcher.Tuple(
            CqlIdentifier.fromInternal("key"),
            new PrimitiveType(ProtocolConstants.DataType.TINYINT),
            new PrimitiveType(ProtocolConstants.DataType.VARCHAR));
    columnsPredicate =
        new CqlColumnMatcher.BasicType(
                CqlIdentifier.fromInternal("tx_id"),
                new PrimitiveType(ProtocolConstants.DataType.TIMEUUID))
            .or(
                new CqlColumnMatcher.Tuple(
                    CqlIdentifier.fromInternal("key"),
                    new PrimitiveType(ProtocolConstants.DataType.TINYINT),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new CqlColumnMatcher.BasicType(
                    CqlIdentifier.fromInternal("doc_json"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new CqlColumnMatcher.Set(
                    CqlIdentifier.fromInternal("exist_keys"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new CqlColumnMatcher.Map(
                    CqlIdentifier.fromInternal("array_size"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                    new PrimitiveType(ProtocolConstants.DataType.INT)))
            .or(
                new CqlColumnMatcher.Set(
                    CqlIdentifier.fromInternal("array_contains"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new CqlColumnMatcher.Map(
                    CqlIdentifier.fromInternal("query_bool_values"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                    new PrimitiveType(ProtocolConstants.DataType.TINYINT)))
            .or(
                new CqlColumnMatcher.Map(
                    CqlIdentifier.fromInternal("query_dbl_values"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                    new PrimitiveType(ProtocolConstants.DataType.DECIMAL)))
            .or(
                new CqlColumnMatcher.Map(
                    CqlIdentifier.fromInternal("query_text_values"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new CqlColumnMatcher.Map(
                    CqlIdentifier.fromInternal("query_timestamp_values"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                    new PrimitiveType(ProtocolConstants.DataType.TIMESTAMP)))
            .or(
                new CqlColumnMatcher.Set(
                    CqlIdentifier.fromInternal("query_null_values"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)));

    columnsPredicateVector =
        new CqlColumnMatcher.BasicType(
                CqlIdentifier.fromInternal("tx_id"),
                new PrimitiveType(ProtocolConstants.DataType.TIMEUUID))
            .or(
                new CqlColumnMatcher.Tuple(
                    CqlIdentifier.fromInternal("key"),
                    new PrimitiveType(ProtocolConstants.DataType.TINYINT),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new CqlColumnMatcher.BasicType(
                    CqlIdentifier.fromInternal("doc_json"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new CqlColumnMatcher.Set(
                    CqlIdentifier.fromInternal("exist_keys"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new CqlColumnMatcher.Map(
                    CqlIdentifier.fromInternal("array_size"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                    new PrimitiveType(ProtocolConstants.DataType.INT)))
            .or(
                new CqlColumnMatcher.Set(
                    CqlIdentifier.fromInternal("array_contains"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new CqlColumnMatcher.Map(
                    CqlIdentifier.fromInternal("query_bool_values"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                    new PrimitiveType(ProtocolConstants.DataType.TINYINT)))
            .or(
                new CqlColumnMatcher.Map(
                    CqlIdentifier.fromInternal("query_dbl_values"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                    new PrimitiveType(ProtocolConstants.DataType.DECIMAL)))
            .or(
                new CqlColumnMatcher.Map(
                    CqlIdentifier.fromInternal("query_text_values"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new CqlColumnMatcher.Map(
                    CqlIdentifier.fromInternal("query_timestamp_values"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR),
                    new PrimitiveType(ProtocolConstants.DataType.TIMESTAMP)))
            .or(
                new CqlColumnMatcher.Set(
                    CqlIdentifier.fromInternal("query_null_values"),
                    new PrimitiveType(ProtocolConstants.DataType.VARCHAR)))
            .or(
                new CqlColumnMatcher.Vector(
                    CqlIdentifier.fromInternal("query_vector_value"),
                    new PrimitiveType(ProtocolConstants.DataType.FLOAT)));
  }

  /**
   * Tests if the given table is a valid jsonapi table.
   *
   * @param cqlTable the table
   * @return Returns true only if all the columns in the table are corresponding the jsonapi table
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
