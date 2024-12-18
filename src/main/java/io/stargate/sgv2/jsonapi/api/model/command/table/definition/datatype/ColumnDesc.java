package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.ColumnDescDeserializer;
import io.stargate.sgv2.jsonapi.api.model.command.serializer.ColumnDescSerializer;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import java.util.Objects;

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

  /**
   * Wrapper for a {@link ColumnDesc} that is for a static column.
   *
   * <p>These are not supported for create, and the concept is not well-supported in to Api schema
   * because of that so we use this wrapper to change the description of the column at the last
   * minute.
   */
  class StaticColumnDesc implements ColumnDesc {

    private final ColumnDesc columnDesc;

    /**
     * Creates a new instance that will wrap the existing {@link ColumnDesc}.
     *
     * <p>All methods will delegate to the wrapped instance, except {@link #apiSupport()} which
     * modifies the results from the wrapped instance.
     *
     * @param columnDesc the column description to wrap
     */
    public StaticColumnDesc(ColumnDesc columnDesc) {
      this.columnDesc = Objects.requireNonNull(columnDesc, "columnDesc must not be null");
    }

    @Override
    public ApiTypeName typeName() {
      return columnDesc.typeName();
    }

    /**
     * Returns the wrapped column's {@link ApiSupportDesc} with the createTable flag set to false
     * and the cqlDefinition set to "static " + the wrapped column's cqlDefinition. Because static
     * columns cannot be created, and there no easy way to express static in CQL (it's a property of
     * the column not type)
     *
     * @return A {@link ApiSupportDesc} describing the static type
     */
    @Override
    public ApiSupportDesc apiSupport() {
      return new ApiSupportDesc(
          false,
          columnDesc.apiSupport().insert(),
          columnDesc.apiSupport().read(),
          columnDesc.apiSupport().filter(),
          "static " + columnDesc.apiSupport().cqlDefinition());
    }
  }
}
