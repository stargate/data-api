package io.stargate.sgv2.jsonapi.service.schema.tables.factories;

import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.UnsupportedColumnDesc;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiSupportDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;

/**
 * A that we do not support in the API, this could have come from the user request or from a CQL
 * table.
 */
public abstract class UnsupportedApiDataType implements ApiDataType {

  // for now do not know what level we have of support other than none
  protected static final ApiSupportDef API_SUPPORT = ApiSupportDef.Support.NONE;

  protected UnsupportedApiDataType() {}

  @Override
  public ApiTypeName typeName() {
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
  public ApiSupportDef apiSupport() {
    return API_SUPPORT;
  }
}
