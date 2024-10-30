package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.ColumnDescDeserializer;
import io.stargate.sgv2.jsonapi.api.model.command.serializer.ColumnDescSerializer;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;

/**
 * Describes a column in a table definition, from the user perspective.
 *
 * <p>All descriptions of schema etc the user gives use are called `Desc` classes.
 */
@JsonDeserialize(using = ColumnDescDeserializer.class)
@JsonSerialize(using = ColumnDescSerializer.class)
public interface ColumnDesc {

  /**
   * Gets the type name of the column.
   *
   * <p>Use this only if you know the column is a supported type, otherwise use {@link #typeName()}.
   * And use that if you just want the name of the type to use in a response.
   *
   * @return the type name of the column as enum, this must be a supported type.
   */
  ApiTypeName typeName();

  /**
   * Gets the string name of the apiType, this is here so that the implementation for unsupported
   * types can return a string and not need to have an unsupported {@link ApiTypeName}
   *
   * @return the name of the apiType as a string, this can be supported and unsupported types.
   */
  default String getApiName() {
    return typeName().apiName();
  }

  /**
   * Gets the support level of the column, normally only used with unsupported types.
   *
   * @return
   */
  ApiSupportDesc apiSupport();
}
