package io.stargate.sgv2.jsonapi.mock;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.util.ArrayList;
import java.util.List;

/** Helpers to list the CQL types we support to drive test data generation. */
public abstract class CqlTypes {

  public static final List<DataType> ALL_NUMERIC_TYPES;
  public static final List<DataType> ALL_TEXT_TYPES;
  public static final List<DataType> ALL_SCALAR_TYPES;

  static {
    ALL_NUMERIC_TYPES =
        List.of(
            DataTypes.BIGINT,
            DataTypes.DECIMAL,
            DataTypes.DOUBLE,
            DataTypes.FLOAT,
            DataTypes.INT,
            DataTypes.SMALLINT,
            DataTypes.TINYINT,
            DataTypes.VARINT);

    ALL_TEXT_TYPES = List.of(DataTypes.TEXT);

    var all = new ArrayList<>(ALL_NUMERIC_TYPES);
    all.addAll(ALL_TEXT_TYPES);
    ALL_SCALAR_TYPES = List.copyOf(all);
  }
}
