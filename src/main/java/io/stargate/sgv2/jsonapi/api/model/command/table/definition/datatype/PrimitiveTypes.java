package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;

/** Interface for primitive column types similar to what is defined in cassandra java driver. */
public class PrimitiveTypes {

  // TODO: add a private ctor to stop this class from being instantiated or make abstract

  public static final ColumnType TEXT = new Text();
  public static final ColumnType INT = new Int();

  public static final ColumnType BOOLEAN = new Boolean();
  public static final ColumnType BIGINT = new BigInt();
  public static final ColumnType DATE = new Date();
  public static final ColumnType DECIMAL = new Decimal();
  public static final ColumnType DOUBLE = new Double();
  public static final ColumnType DURATION = new Duration();
  public static final ColumnType FLOAT = new Float();
  public static final ColumnType SMALLINT = new SmallInt();
  public static final ColumnType TIME = new Time();
  public static final ColumnType TIMESTAMP = new Timestamp();
  public static final ColumnType TINYINT = new TinyInt();
  public static final ColumnType VARINT = new VarInt();
  public static final ColumnType ASCII = new Ascii();
  public static final ColumnType BLOB = new Blob();

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

  private static class Boolean implements ColumnType {
    @Override
    public DataType getCqlType() {
      return DataTypes.BOOLEAN;
    }
  }

  private static class BigInt implements ColumnType {
    @Override
    public DataType getCqlType() {
      return DataTypes.BIGINT;
    }
  }

  private static class Date implements ColumnType {
    @Override
    public DataType getCqlType() {
      return DataTypes.DATE;
    }
  }

  private static class Decimal implements ColumnType {
    @Override
    public DataType getCqlType() {
      return DataTypes.DECIMAL;
    }
  }

  private static class Double implements ColumnType {
    @Override
    public DataType getCqlType() {
      return DataTypes.DOUBLE;
    }
  }

  private static class Duration implements ColumnType {
    @Override
    public DataType getCqlType() {
      return DataTypes.DURATION;
    }
  }

  private static class Float implements ColumnType {
    @Override
    public DataType getCqlType() {
      return DataTypes.FLOAT;
    }
  }

  private static class SmallInt implements ColumnType {
    @Override
    public DataType getCqlType() {
      return DataTypes.SMALLINT;
    }
  }

  private static class Time implements ColumnType {
    @Override
    public DataType getCqlType() {
      return DataTypes.TIME;
    }
  }

  private static class Timestamp implements ColumnType {
    @Override
    public DataType getCqlType() {
      return DataTypes.TIMESTAMP;
    }
  }

  private static class TinyInt implements ColumnType {
    @Override
    public DataType getCqlType() {
      return DataTypes.TINYINT;
    }
  }

  private static class VarInt implements ColumnType {
    @Override
    public DataType getCqlType() {
      return DataTypes.VARINT;
    }
  }

  private static class Ascii implements ColumnType {
    @Override
    public DataType getCqlType() {
      return DataTypes.ASCII;
    }
  }

  private static class Blob implements ColumnType {
    @Override
    public DataType getCqlType() {
      return DataTypes.BLOB;
    }
  }
}
