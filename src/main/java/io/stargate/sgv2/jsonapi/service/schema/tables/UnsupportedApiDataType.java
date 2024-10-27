package io.stargate.sgv2.jsonapi.service.schema.tables;

public abstract class UnsupportedApiDataType implements ApiDataType {

  protected UnsupportedApiDataType() {}

  @Override
  public ApiDataTypeName getName() {
    throw new UnsupportedOperationException("UnsupportedCqlApiDataType does not have a name");
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
