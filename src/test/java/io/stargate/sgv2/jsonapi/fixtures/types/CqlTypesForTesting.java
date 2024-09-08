package io.stargate.sgv2.jsonapi.fixtures.types;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.util.ArrayList;
import java.util.List;

/**
 * Helpers to list the CQL types we support to drive test data generation. This is <b>NOT</b>
 * intended to be the list of supported types of the API, this is driven from when the {@link
 * io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistry} supports.
 */
public abstract class CqlTypesForTesting {

  // NOTE: Using the static ctor because build lists of the lists.
  public static final List<DataType> NUMERIC_TYPES;
  public static final List<DataType> TEXT_TYPES;
  public static final List<DataType> SCALAR_TYPES;

  public static final List<DataType> SUPPORTED_FOR_INSERT;
  public static final List<DataType> UNSUPPORTED_FOR_INSERT;

  public static final List<DataType> OVERFLOW_TYPES;
  public static final List<DataType> UNDERFLOW_TYPES;
  public static final List<DataType> INFINITY_TYPES = List.of(DataTypes.DOUBLE, DataTypes.FLOAT);
  static {
    NUMERIC_TYPES =
        List.of(
            DataTypes.BIGINT,
            DataTypes.DECIMAL,
            DataTypes.DOUBLE,
            DataTypes.FLOAT,
            DataTypes.INT,
            DataTypes.SMALLINT,
            DataTypes.TINYINT,
            DataTypes.VARINT);

    TEXT_TYPES = List.of(DataTypes.TEXT);

    var all = new ArrayList<>(NUMERIC_TYPES);
    all.addAll(TEXT_TYPES);
    SCALAR_TYPES = List.copyOf(all);

    // aaron 3 aug 2024 - type of the collection types should not be too important, more that it is
    // a collection.
    UNSUPPORTED_FOR_INSERT =
        List.of(
            DataTypes.COUNTER,
            DataTypes.listOf(DataTypes.INT),
            DataTypes.listOf(DataTypes.INT, true),
            DataTypes.setOf(DataTypes.TEXT),
            DataTypes.setOf(DataTypes.TEXT, true),
            DataTypes.mapOf(DataTypes.TEXT, DataTypes.DOUBLE),
            DataTypes.mapOf(DataTypes.TEXT, DataTypes.DOUBLE, true));

    SUPPORTED_FOR_INSERT = new ArrayList<>(SCALAR_TYPES);

    OVERFLOW_TYPES = List.of(
        DataTypes.DOUBLE,
        DataTypes.FLOAT,
        DataTypes.INT,
        DataTypes.SMALLINT,
        DataTypes.TINYINT);
    UNDERFLOW_TYPES = List.copyOf(OVERFLOW_TYPES);
  }

  // Helper method that is easier to use in filter()
  public static boolean isSupportedForInsert(DataType type) {
    return SUPPORTED_FOR_INSERT.contains(type);
  }

  // Helper method that is easier to use in filter()
  public static boolean isSupportedForInsert(ColumnMetadata metadata) {
    return isSupportedForInsert(metadata.getType());
  }
}
