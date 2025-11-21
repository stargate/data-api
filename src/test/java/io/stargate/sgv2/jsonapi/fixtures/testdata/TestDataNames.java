package io.stargate.sgv2.jsonapi.fixtures.testdata;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import java.util.List;

public class TestDataNames {

  public final CqlIdentifier KEYSPACE_NAME =
      CqlIdentifier.fromInternal("keyspace_" + System.currentTimeMillis());

  public final CqlIdentifier TABLE_NAME =
      CqlIdentifier.fromInternal("table_" + System.currentTimeMillis());

  public final CqlIdentifier INDEX_NAME_1 =
      CqlIdentifier.fromInternal("index_1_" + System.currentTimeMillis());

  public final CqlIdentifier COL_PARTITION_KEY_1 =
      CqlIdentifier.fromInternal("partition_key_1_" + System.currentTimeMillis());
  public final CqlIdentifier COL_PARTITION_KEY_2 =
      CqlIdentifier.fromInternal("partition_key_2_" + System.currentTimeMillis());
  public final CqlIdentifier COL_CLUSTERING_KEY_1 =
      CqlIdentifier.fromInternal("clustering_key_1_" + System.currentTimeMillis());
  public final CqlIdentifier COL_CLUSTERING_KEY_2 =
      CqlIdentifier.fromInternal("clustering_key_2_" + System.currentTimeMillis());
  public final CqlIdentifier COL_CLUSTERING_KEY_3 =
      CqlIdentifier.fromInternal("clustering_key_3_" + System.currentTimeMillis());

  public final CqlIdentifier COL_REGULAR_1 =
      CqlIdentifier.fromInternal("regular_1_" + System.currentTimeMillis());
  public final CqlIdentifier COL_REGULAR_2 =
      CqlIdentifier.fromInternal("regular_2_" + System.currentTimeMillis());

  public final CqlIdentifier COL_INDEXED_1 =
      CqlIdentifier.fromInternal("indexed_1_" + System.currentTimeMillis());

  // DO NOT ADD TO A TABLE
  public final CqlIdentifier COL_UNKNOWN_1 =
      CqlIdentifier.fromInternal("unknown_1_" + System.currentTimeMillis());

  // ==================================================================================================================
  // Primitive(Scalar) dataTypes
  // ==================================================================================================================

  public final CqlIdentifier CQL_TEXT_COLUMN =
      CqlIdentifier.fromInternal("text_column_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_ASCII_COLUMN =
      CqlIdentifier.fromInternal("ascii_column_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_BLOB_COLUMN =
      CqlIdentifier.fromInternal("blob_column_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_DURATION_COLUMN =
      CqlIdentifier.fromInternal("duration_column_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_BOOLEAN_COLUMN =
      CqlIdentifier.fromInternal("boolean_column_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_BIGINT_COLUMN =
      CqlIdentifier.fromInternal("bigint_column_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_DECIMAL_COLUMN =
      CqlIdentifier.fromInternal("decimal_column_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_DOUBLE_COLUMN =
      CqlIdentifier.fromInternal("double_column_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_FLOAT_COLUMN =
      CqlIdentifier.fromInternal("float_column_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_INT_COLUMN =
      CqlIdentifier.fromInternal("int_column_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_SMALLINT_COLUMN =
      CqlIdentifier.fromInternal("smallint_column_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_VARINT_COLUMN =
      CqlIdentifier.fromInternal("varint_column_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_TINYINT_COLUMN =
      CqlIdentifier.fromInternal("tinyint_column_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_TIMESTAMP_COLUMN =
      CqlIdentifier.fromInternal("timestamp_column" + System.currentTimeMillis());
  public final CqlIdentifier CQL_DATE_COLUMN =
      CqlIdentifier.fromInternal("date_column_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_TIME_COLUMN =
      CqlIdentifier.fromInternal("time_column_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_INET_COLUMN =
      CqlIdentifier.fromInternal("inet_column_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_UUID_COLUMN =
      CqlIdentifier.fromInternal("uuid_column_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_TIMEUUID_COLUMN =
      CqlIdentifier.fromInternal("timeuuid_column_" + System.currentTimeMillis());

  public final List<CqlIdentifier> ALL_SCALAR_DATATYPE_COLUMNS =
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
      CqlIdentifier.fromInternal("text_column_index_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_ASCII_COLUMN_INDEX =
      CqlIdentifier.fromInternal("ascii_column_index_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_BOOLEAN_COLUMN_INDEX =
      CqlIdentifier.fromInternal("boolean_column_index_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_BIGINT_COLUMN_INDEX =
      CqlIdentifier.fromInternal("bigint_column_index_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_DECIMAL_COLUMN_INDEX =
      CqlIdentifier.fromInternal("decimal_column_index_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_DOUBLE_COLUMN_INDEX =
      CqlIdentifier.fromInternal("double_column_index_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_FLOAT_COLUMN_INDEX =
      CqlIdentifier.fromInternal("float_column_index_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_INT_COLUMN_INDEX =
      CqlIdentifier.fromInternal("int_column_index_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_SMALLINT_COLUMN_INDEX =
      CqlIdentifier.fromInternal("smallint_column_index_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_VARINT_COLUMN_INDEX =
      CqlIdentifier.fromInternal("varint_column_index_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_TINYINT_COLUMN_INDEX =
      CqlIdentifier.fromInternal("tinyint_column_index_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_TIMESTAMP_COLUMN_INDEX =
      CqlIdentifier.fromInternal("timestamp_column_index_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_DATE_COLUMN_INDEX =
      CqlIdentifier.fromInternal("date_column_index_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_TIME_COLUMN_INDEX =
      CqlIdentifier.fromInternal("time_column_index_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_INET_COLUMN_INDEX =
      CqlIdentifier.fromInternal("inet_column_index_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_UUID_COLUMN_INDEX =
      CqlIdentifier.fromInternal("uuid_column_index_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_TIMEUUID_COLUMN_INDEX =
      CqlIdentifier.fromInternal("timeuuid_column_index_" + System.currentTimeMillis());

  // ==================================================================================================================
  // Collection(set, map, list, vector) dataTypes
  // ==================================================================================================================

  public final CqlIdentifier CQL_MAP_COLUMN =
      CqlIdentifier.fromInternal("map_column_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_LIST_COLUMN =
      CqlIdentifier.fromInternal("list_column_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_SET_COLUMN =
      CqlIdentifier.fromInternal("set_column_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_VECTOR_COLUMN =
      CqlIdentifier.fromInternal("vector_column_" + System.currentTimeMillis());

  // UDT columns
  public final CqlIdentifier CQL_NON_FROZEN_UDT_COLUMN_ADDRESS =
      CqlIdentifier.fromInternal("non_frozen_address_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_ADDRESS_CITY_FIELD =
      CqlIdentifier.fromInternal("address_city_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_ADDRESS_COUNTRY_FIELD =
      CqlIdentifier.fromInternal("address_country_" + System.currentTimeMillis());

  public final CqlIdentifier CQL_MAP_COLUMN_INDEX =
      CqlIdentifier.fromInternal("map_column_index_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_LIST_COLUMN_INDEX =
      CqlIdentifier.fromInternal("list_column_index_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_SET_COLUMN_INDEX =
      CqlIdentifier.fromInternal("set_column_index_" + System.currentTimeMillis());
  public final CqlIdentifier CQL_VECTOR_COLUMN_INDEX =
      CqlIdentifier.fromInternal("vector_column_index_" + System.currentTimeMillis());
}
