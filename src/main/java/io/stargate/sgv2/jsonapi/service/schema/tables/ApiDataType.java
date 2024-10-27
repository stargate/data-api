package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataType;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;

/** TODO WORDS */
@JsonSerialize(using = ApiDataTypeDefSerializer.class)
public interface ApiDataType {

  ApiDataTypeName typeName();

  /**
   * Gets the API name of the type.
   *
   * <p>This is here because {@link UnsupportedApiDataType} will not have a {@link ApiDataTypeName}
   * so call this if you need the name of the type as a string so the UnsupportedApiDataType can
   * return a string.
   *
   * @return
   */
  default String apiName() {
    return typeName().getApiName();
  }

  DataType cqlType();

  boolean isPrimitive();

  boolean isContainer();

  boolean isUnsupported();

  ColumnDesc columnDesc();
}
