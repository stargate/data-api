package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataType;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.stargate.sgv2.jsonapi.api.model.command.CommandType;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescribable;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.service.schema.tables.factories.UnsupportedApiDataType;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;

/**
 * TODO: WORDS
 *
 * <p><b>NOTE:</b> Even though this class is {@link SchemaDescribable}, the static flag is on the
 * column not the type, you should normally call {@link
 * ApiColumnDef#getSchemaDescription(CommandType)} because it will handle static columns.
 */
@JsonSerialize(using = ApiDataTypeDefSerializer.class)
public interface ApiDataType extends SchemaDescribable<ColumnDesc>, Recordable {

  ApiTypeName typeName();

  /**
   * Gets the API name of the type.
   *
   * <p>This is here because {@link UnsupportedApiDataType} will not have a {@link ApiTypeName} so
   * call this if you need the name of the type as a string so the UnsupportedApiDataType can return
   * a string.
   *
   * @return
   */
  default String apiName() {
    return typeName().apiName();
  }

  DataType cqlType();

  ApiSupportDef apiSupport();

  default boolean isPrimitive() {
    return typeName().isPrimitive();
  }

  default boolean isContainer() {
    return typeName().isContainer();
  }

  @Override
  default Recordable.DataRecorder recordTo(Recordable.DataRecorder dataRecorder) {
    return dataRecorder
        .append("apiName", apiName())
        .append("apiSupport", apiSupport())
        .append("cqlType", cqlType());
  }
}
