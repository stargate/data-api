package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeName;

/**
 * Unsupported type implementation, returned in response when cql table has unsupported format
 * column
 */
public abstract class UnsupportedColumnDesc implements ColumnDesc {

  public static final String UNSUPPORTED_TYPE_NAME = "UNSUPPORTED";

  protected UnsupportedColumnDesc() {}

  @Override
  public ApiDataTypeName getApiDataTypeName() {
    throw new UnsupportedOperationException("Unsupported type");
  }

  @Override
  public String getApiName() {
    return UNSUPPORTED_TYPE_NAME;
  }

  public abstract String cqlFormat();

  public static class UnsupportedCqlColumnDesc extends UnsupportedColumnDesc {
    private final String cqlFormat;

    public UnsupportedCqlColumnDesc(DataType cqlType) {
      this.cqlFormat = cqlType.asCql(true, true);
    }

    public String cqlFormat() {
      return cqlFormat;
    }
  }

  public static class UnsupportedUserColumnDesc extends UnsupportedColumnDesc {
    public UnsupportedUserColumnDesc() {}

    @Override
    public String cqlFormat() {
      throw new UnsupportedOperationException();
    }
  }
}
