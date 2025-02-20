package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataType;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.util.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.PrettyToStringBuilder;

/** TODO WORDS */
@JsonSerialize(using = ApiDataTypeDefSerializer.class)
public interface ApiDataType  extends PrettyPrintable {

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

  boolean isPrimitive();

  boolean isContainer();

  ApiSupportDef apiSupport();

  /**
   * Called to get the user API description of the type.
   *
   * <p><b>NOTE:</b> Because the static flag is on the column not the type, you should normally call
   * {@link ApiColumnDef#columnDesc()} because it will handle static columns.
   *
   * @return {@link ColumnDesc} that describes the data type to the user.
   */
  ColumnDesc columnDesc();

  @Override
  default PrettyToStringBuilder toString(PrettyToStringBuilder prettyToStringBuilder) {
    prettyToStringBuilder
        .append("apiName", apiName())
        .append("apiSupport", apiSupport());
    return prettyToStringBuilder;
  }
}
