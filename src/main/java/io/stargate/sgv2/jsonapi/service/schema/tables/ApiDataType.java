package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataType;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnType;

/** TODO WORDS */
@JsonSerialize(using = ApiDataTypeDefSerializer.class)
public interface ApiDataType {

  ApiDataTypeName getName();

  DataType getCqlType();

  boolean isPrimitive();

  boolean isContainer();

  boolean isUnsupported();

  ColumnType getColumnType();
}
