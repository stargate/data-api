package io.stargate.sgv2.jsonapi.service.schema.tables;

import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.UnsupportedColumnDesc;

public abstract class UnsupportedApiDataType implements ApiDataType {

  protected UnsupportedApiDataType() {}

  @Override
  public ApiDataTypeName typeName() {
    throw new UnsupportedOperationException("UnsupportedCqlApiDataType does not have a name");
  }

  @Override
  public String apiName() {
    return UnsupportedColumnDesc.UNSUPPORTED_TYPE_NAME;
  }

  @Override
  public boolean isPrimitive() {
    return false;
  }

  @Override
  public boolean isContainer() {
    return false;
  }

  @Override
  public boolean isUnsupported() {
    return true;
  }
}
