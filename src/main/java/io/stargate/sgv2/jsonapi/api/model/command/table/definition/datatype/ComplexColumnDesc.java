package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.Objects;

/** Interface for complex column types like collections */
public abstract class ComplexColumnDesc implements ColumnDesc {

  private final ApiTypeName apiTypeName;
  private final ApiSupportDesc apiSupportDesc;

  protected ComplexColumnDesc(ApiTypeName apiTypeName, ApiSupportDesc apiSupportDesc) {
    this.apiTypeName = Objects.requireNonNull(apiTypeName, "apiTypeName must not be null");
    this.apiSupportDesc = Objects.requireNonNull(apiSupportDesc, "apiSupportDesc must not be null");
    ;
  }

  @Override
  public ApiTypeName typeName() {
    return apiTypeName;
  }

  @Override
  public ApiSupportDesc apiSupport() {
    return apiSupportDesc;
  }
}
