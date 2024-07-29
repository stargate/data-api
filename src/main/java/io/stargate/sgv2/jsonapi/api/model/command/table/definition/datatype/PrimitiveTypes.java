package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;

/** Interface for primitive column types similar to what is defined in cassandra java driver. */
public class PrimitiveTypes {

  public static final ColumnType TEXT = new Text();
  public static final ColumnType INT = new Int();

  private static class Text implements ColumnType {
    @Override
    public DataType getCqlType() {
      return DataTypes.TEXT;
    }
  }

  private static class Int implements ColumnType {
    @Override
    public DataType getCqlType() {
      return DataTypes.INT;
    }
  }
}
