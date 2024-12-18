package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiSupportDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;

/**
 * Unsupported type implementation, returned in response when cql table has unsupported format
 * column
 */
public abstract class UnsupportedColumnDesc implements ColumnDesc {

  public static final String UNSUPPORTED_TYPE_NAME = "UNSUPPORTED";

  private final ApiSupportDesc apiSupportDesc;

  protected UnsupportedColumnDesc(ApiSupportDesc apiSupportDesc) {
    this.apiSupportDesc = apiSupportDesc;
  }

  @Override
  public ApiTypeName typeName() {
    throw new UnsupportedOperationException("Unsupported type");
  }

  @Override
  public String getApiName() {
    return UNSUPPORTED_TYPE_NAME;
  }

  @Override
  public ApiSupportDesc apiSupport() {
    return apiSupportDesc;
  }

  public static class UnsupportedCqlColumnDesc extends UnsupportedColumnDesc {

    public UnsupportedCqlColumnDesc(ApiSupportDef apiSupportDef, DataType cqlType) {
      super(ApiSupportDesc.from(apiSupportDef, cqlType));
    }
  }

  public static class UnsupportedUserColumnDesc extends UnsupportedColumnDesc {
    public UnsupportedUserColumnDesc() {
      super(ApiSupportDesc.withoutCqlDefinition(ApiSupportDef.Support.NONE));
    }
  }
}
