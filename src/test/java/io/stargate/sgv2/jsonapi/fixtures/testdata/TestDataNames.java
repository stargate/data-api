package io.stargate.sgv2.jsonapi.fixtures.testdata;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import java.util.List;

public class TestDataNames {

  public final CqlIdentifier KEYSPACE_NAME =
      CqlIdentifier.fromInternal("keyspace-" + System.currentTimeMillis());

  public final CqlIdentifier TABLE_NAME =
      CqlIdentifier.fromInternal("table-" + System.currentTimeMillis());

  public final CqlIdentifier INDEX_NAME_1 =
      CqlIdentifier.fromInternal("index-1-" + System.currentTimeMillis());

  public final CqlIdentifier COL_PARTITION_KEY_1 =
      CqlIdentifier.fromInternal("partition-key-1-" + System.currentTimeMillis());
  public final CqlIdentifier COL_PARTITION_KEY_2 =
      CqlIdentifier.fromInternal("partition-key-2-" + System.currentTimeMillis());
  public final CqlIdentifier COL_CLUSTERING_KEY_1 =
      CqlIdentifier.fromInternal("clustering-key-1-" + System.currentTimeMillis());
  public final CqlIdentifier COL_CLUSTERING_KEY_2 =
      CqlIdentifier.fromInternal("clustering-key-2-" + System.currentTimeMillis());
  public final CqlIdentifier COL_CLUSTERING_KEY_3 =
      CqlIdentifier.fromInternal("clustering-key-3-" + System.currentTimeMillis());

  public final CqlIdentifier COL_REGULAR_1 =
      CqlIdentifier.fromInternal("regular-1-" + System.currentTimeMillis());
  public final CqlIdentifier COL_REGULAR_2 =
      CqlIdentifier.fromInternal("regular-2-" + System.currentTimeMillis());

  public final CqlIdentifier COL_INDEXED_1 =
      CqlIdentifier.fromInternal("indexed-1-" + System.currentTimeMillis());

  // DO NOT ADD TO A TABLE
  public final CqlIdentifier COL_UNKNOWN_1 =
      CqlIdentifier.fromInternal("unknown-1-" + System.currentTimeMillis());

  // All column datatypes
  public final CqlIdentifier CQL_TEXT_COLUMN =
      CqlIdentifier.fromInternal("text-column-" + System.currentTimeMillis());
  // TODO  public final CqlIdentifier CQL_UUID_COLUMN =
  //          CqlIdentifier.fromInternal("uuid-column-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_ASCII_COLUMN =
      CqlIdentifier.fromInternal("ascii-column-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_BLOB_COLUMN =
      CqlIdentifier.fromInternal("blob-column-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_DURATION_COLUMN =
      CqlIdentifier.fromInternal("duration-column-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_BOOLEAN_COLUMN =
      CqlIdentifier.fromInternal("boolean-column-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_BIGINT_COLUMN =
      CqlIdentifier.fromInternal("bigint-column-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_DECIMAL_COLUMN =
      CqlIdentifier.fromInternal("decimal-column-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_DOUBLE_COLUMN =
      CqlIdentifier.fromInternal("double-column-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_FLOAT_COLUMN =
      CqlIdentifier.fromInternal("float-column-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_INT_COLUMN =
      CqlIdentifier.fromInternal("int-column-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_SMALLINT_COLUMN =
      CqlIdentifier.fromInternal("smallint-column-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_VARINT_COLUMN =
      CqlIdentifier.fromInternal("varint-column-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_TINYINT_COLUMN =
      CqlIdentifier.fromInternal("tinyint-column-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_TIMESTAMP_COLUMN =
      CqlIdentifier.fromInternal("timestamp-column-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_DATE_COLUMN =
      CqlIdentifier.fromInternal("date-column-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_TIME_COLUMN =
      CqlIdentifier.fromInternal("time-column-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_INET_COLUMN =
      CqlIdentifier.fromInternal("inet-column-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_UUID_COLUMN =
      CqlIdentifier.fromInternal("uuid-column-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_TIMEUUID_COLUMN =
      CqlIdentifier.fromInternal("timeuuid-column-" + System.currentTimeMillis());

  public final List<CqlIdentifier> ALL_CQL_DATATYPE_COLUMNS =
      List.of(
          CQL_TEXT_COLUMN,
          CQL_ASCII_COLUMN,
          CQL_BLOB_COLUMN,
          CQL_DURATION_COLUMN,
          CQL_BOOLEAN_COLUMN,
          CQL_BIGINT_COLUMN,
          CQL_DECIMAL_COLUMN,
          CQL_DOUBLE_COLUMN,
          CQL_FLOAT_COLUMN,
          CQL_INT_COLUMN,
          CQL_SMALLINT_COLUMN,
          CQL_VARINT_COLUMN,
          CQL_TINYINT_COLUMN,
          CQL_TIMESTAMP_COLUMN,
          CQL_DATE_COLUMN,
          CQL_TIME_COLUMN,
          CQL_INET_COLUMN,
          CQL_UUID_COLUMN,
          CQL_TIMEUUID_COLUMN);

  public final List<CqlIdentifier> COMPARISON_WITH_INDEX_WARN_COLUMNS =
      List.of(CQL_TEXT_COLUMN, CQL_ASCII_COLUMN, CQL_BOOLEAN_COLUMN, CQL_UUID_COLUMN);

  // All column datatypes index name, (Blob,Duration can NOT be indexed)
  public final CqlIdentifier CQL_TEXT_COLUMN_INDEX =
      CqlIdentifier.fromInternal("text-column-index-" + System.currentTimeMillis());
  // TODO  public final CqlIdentifier CQL_UUID_COLUMN_INDEX =
  //          CqlIdentifier.fromInternal("uuid-column-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_ASCII_COLUMN_INDEX =
      CqlIdentifier.fromInternal("ascii-column-index-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_BOOLEAN_COLUMN_INDEX =
      CqlIdentifier.fromInternal("boolean-column-index-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_BIGINT_COLUMN_INDEX =
      CqlIdentifier.fromInternal("bigint-column-index-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_DECIMAL_COLUMN_INDEX =
      CqlIdentifier.fromInternal("decimal-column-index-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_DOUBLE_COLUMN_INDEX =
      CqlIdentifier.fromInternal("double-column-index-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_FLOAT_COLUMN_INDEX =
      CqlIdentifier.fromInternal("float-column-index-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_INT_COLUMN_INDEX =
      CqlIdentifier.fromInternal("int-column-index-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_SMALLINT_COLUMN_INDEX =
      CqlIdentifier.fromInternal("smallint-column-index-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_VARINT_COLUMN_INDEX =
      CqlIdentifier.fromInternal("varint-column-index-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_TINYINT_COLUMN_INDEX =
      CqlIdentifier.fromInternal("tinyint-column-index-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_TIMESTAMP_COLUMN_INDEX =
      CqlIdentifier.fromInternal("timestamp-column-index-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_DATE_COLUMN_INDEX =
      CqlIdentifier.fromInternal("date-column-index-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_TIME_COLUMN_INDEX =
      CqlIdentifier.fromInternal("time-column-index-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_INET_COLUMN_INDEX =
      CqlIdentifier.fromInternal("inet-column-index-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_UUID_COLUMN_INDEX =
      CqlIdentifier.fromInternal("uuid-column-index-" + System.currentTimeMillis());
  public final CqlIdentifier CQL_TIMEUUID_COLUMN_INDEX =
      CqlIdentifier.fromInternal("timeuuid-column-index-" + System.currentTimeMillis());
}
