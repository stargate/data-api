package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescSource;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.Objects;

/**
 * Interface for descriptions of columns that are not a {@link PrimitiveColumnDesc}.
 *
 * <p>These are things like map, set, list, vector, tuple that have extra information for the desc
 * of the column.
 */
public abstract class ComplexColumnDesc implements ColumnDesc {

  private final ApiTypeName apiTypeName;
  private final ApiSupportDesc apiSupportDesc;

  protected ComplexColumnDesc(SchemaDescSource schemaDescSource, ApiTypeName apiTypeName) {
    this(schemaDescSource, apiTypeName, null);
  }

  protected ComplexColumnDesc(
      SchemaDescSource schemaDescSource, ApiTypeName apiTypeName, ApiSupportDesc apiSupportDesc) {
    this.apiTypeName = Objects.requireNonNull(apiTypeName, "apiTypeName must not be null");
    this.apiSupportDesc = apiSupportDesc;

    if (schemaDescSource.isSystemGenerated()) {
      Objects.requireNonNull(apiSupportDesc, "apiSupportDesc must not be null");
    } else if (apiSupportDesc != null) {
      throw new IllegalArgumentException(
          "apiSupportDesc must be null when schemaDescSource is not system generated, schemaDescSource: "
              + schemaDescSource);
    }
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
