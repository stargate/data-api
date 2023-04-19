package io.stargate.sgv2.jsonapi.service.schema.model;

import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec.Basic;
import io.stargate.bridge.proto.Schema;
import java.util.List;
import java.util.function.Predicate;

/** Simple class that can check if table is a matching jsonapi table. */
public class JsonapiTableMatcher implements Predicate<Schema.CqlTable> {

  private final Predicate<QueryOuterClass.ColumnSpec> primaryKeyPredicate;

  private final Predicate<QueryOuterClass.ColumnSpec> columnsPredicate;

  public JsonapiTableMatcher() {
    primaryKeyPredicate = new CqlColumnMatcher.Tuple("key", Basic.TINYINT, Basic.VARCHAR);
    columnsPredicate =
        new CqlColumnMatcher.BasicType("tx_id", Basic.TIMEUUID)
            .or(new CqlColumnMatcher.BasicType("doc_json", Basic.VARCHAR))
            .or(new CqlColumnMatcher.Set("exist_keys", Basic.VARCHAR))
            .or(new CqlColumnMatcher.Map("sub_doc_equals", Basic.VARCHAR, Basic.VARCHAR))
            .or(new CqlColumnMatcher.Map("array_size", Basic.VARCHAR, Basic.INT))
            .or(new CqlColumnMatcher.Map("array_equals", Basic.VARCHAR, Basic.VARCHAR))
            .or(new CqlColumnMatcher.Set("array_contains", Basic.VARCHAR))
            .or(new CqlColumnMatcher.Map("query_bool_values", Basic.VARCHAR, Basic.TINYINT))
            .or(new CqlColumnMatcher.Map("query_dbl_values", Basic.VARCHAR, Basic.DECIMAL))
            .or(new CqlColumnMatcher.Map("query_text_values", Basic.VARCHAR, Basic.VARCHAR))
            .or(new CqlColumnMatcher.Map("query_timestamp_values", Basic.VARCHAR, Basic.TIMESTAMP))
            .or(new CqlColumnMatcher.Set("query_null_values", Basic.VARCHAR));
  }

  /**
   * Tests if the given table is a valid jsonapi table.
   *
   * @param cqlTable the table
   * @return Returns true only if all the columns in the table are corresponding the jsonapi table
   *     schema.
   */
  @Override
  public boolean test(Schema.CqlTable cqlTable) {
    // null safety
    if (null == cqlTable) {
      return false;
    }

    // partition columns
    List<QueryOuterClass.ColumnSpec> partitionColumns = cqlTable.getPartitionKeyColumnsList();
    if (partitionColumns.size() != 1 || !partitionColumns.stream().allMatch(primaryKeyPredicate)) {
      return false;
    }

    // clustering columns
    List<QueryOuterClass.ColumnSpec> clusteringColumns = cqlTable.getClusteringKeyColumnsList();
    if (clusteringColumns.size() != 0) {
      return false;
    }

    List<QueryOuterClass.ColumnSpec> columns = cqlTable.getColumnsList();
    if (columns.size() != 12 || !columns.stream().allMatch(columnsPredicate)) {
      return false;
    }

    return true;
  }
}
