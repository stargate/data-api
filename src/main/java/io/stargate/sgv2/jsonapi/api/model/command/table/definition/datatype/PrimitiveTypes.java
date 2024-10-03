package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
import io.stargate.sgv2.jsonapi.service.schema.tables.PrimitiveApiDataType;

/** Interface for primitive column types similar to what is defined in cassandra java driver. */
public class PrimitiveTypes {

  // TODO: add a private ctor to stop this class from being instantiated or make abstract

  public static final ColumnType ASCII = new Ascii();
  public static final ColumnType BIGINT = new BigInt();
  public static final ColumnType BINARY = new Binary();
  public static final ColumnType BOOLEAN = new Boolean();
  public static final ColumnType DATE = new Date();
  public static final ColumnType DECIMAL = new Decimal();
  public static final ColumnType DOUBLE = new Double();
  public static final ColumnType DURATION = new Duration();
  public static final ColumnType FLOAT = new Float();
  public static final ColumnType INET = new Inet();
  public static final ColumnType INT = new Int();
  public static final ColumnType SMALLINT = new SmallInt();
  public static final ColumnType TEXT = new Text();
  public static final ColumnType TIME = new Time();
  public static final ColumnType TIMESTAMP = new Timestamp();
  public static final ColumnType TINYINT = new TinyInt();
  public static final ColumnType UUID = new Uuid();
  public static final ColumnType VARINT = new VarInt();

  private static class Text implements ColumnType {
    @Override
    public ApiDataType getApiDataType() {
      return PrimitiveApiDataType.TEXT;
    }
  }

  private static class Int implements ColumnType {
    @Override
    public ApiDataType getApiDataType() {
      return PrimitiveApiDataType.INT;
    }
  }

  private static class Boolean implements ColumnType {
    @Override
    public ApiDataType getApiDataType() {
      return PrimitiveApiDataType.BOOLEAN;
    }
  }

  private static class BigInt implements ColumnType {
    @Override
    public ApiDataType getApiDataType() {
      return PrimitiveApiDataType.BIGINT;
    }
  }

  private static class Decimal implements ColumnType {
    @Override
    public ApiDataType getApiDataType() {
      return PrimitiveApiDataType.DECIMAL;
    }
  }

  private static class Double implements ColumnType {
    @Override
    public ApiDataType getApiDataType() {
      return PrimitiveApiDataType.DOUBLE;
    }
  }

  private static class Float implements ColumnType {
    @Override
    public ApiDataType getApiDataType() {
      return PrimitiveApiDataType.FLOAT;
    }
  }

  private static class SmallInt implements ColumnType {
    @Override
    public ApiDataType getApiDataType() {
      return PrimitiveApiDataType.SMALLINT;
    }
  }

  private static class TinyInt implements ColumnType {
    @Override
    public ApiDataType getApiDataType() {
      return PrimitiveApiDataType.TINYINT;
    }
  }

  private static class VarInt implements ColumnType {
    @Override
    public ApiDataType getApiDataType() {
      return PrimitiveApiDataType.VARINT;
    }
  }

  private static class Ascii implements ColumnType {
    @Override
    public ApiDataType getApiDataType() {
      return PrimitiveApiDataType.ASCII;
    }
  }

  private static class Binary implements ColumnType {
    @Override
    public ApiDataType getApiDataType() {
      return PrimitiveApiDataType.BINARY;
    }
  }

  private static class Date implements ColumnType {
    @Override
    public ApiDataType getApiDataType() {
      return PrimitiveApiDataType.DATE;
    }
  }

  private static class Duration implements ColumnType {
    @Override
    public ApiDataType getApiDataType() {
      return PrimitiveApiDataType.DURATION;
    }
  }

  private static class Time implements ColumnType {
    @Override
    public ApiDataType getApiDataType() {
      return PrimitiveApiDataType.TIME;
    }
  }

  private static class Timestamp implements ColumnType {
    @Override
    public ApiDataType getApiDataType() {
      return PrimitiveApiDataType.TIMESTAMP;
    }
  }

  private static class Inet implements ColumnType {
    @Override
    public ApiDataType getApiDataType() {
      return PrimitiveApiDataType.INET;
    }
  }

  private static class Uuid implements ColumnType {
    @Override
    public ApiDataType getApiDataType() {
      return PrimitiveApiDataType.UUID;
    }
  }
}
