package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.*;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescBindingPoint;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.PrimitiveColumnDesc;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The definition of a type the API supports for a table column.
 *
 * <p>Singleton instances are created in {@link ApiDataTypeDefs} for the supported types.
 *
 * <p>The {@link ApiTypeName} is the names of the types, this class is used to define how the type
 * works and defines the de/serialisation of the type.
 *
 * <p>aaron - 9 sept 2024 - avoiding a record for now as assume will use subclasses for collections
 */
public class PrimitiveApiDataTypeDef implements ApiDataType {
  private static final Logger LOGGER = LoggerFactory.getLogger(PrimitiveApiDataTypeDef.class);

  private final ApiTypeName typeName;
  private final DataType cqlType;
  private final ApiSupportDef apiSupport;
  private final DefaultTypeBindingRules supportBindingRules;

  public PrimitiveApiDataTypeDef(ApiTypeName typeName, DataType cqlType, ApiSupportDef apiSupport) {
    this(typeName, cqlType, apiSupport, DefaultTypeBindingRules.ALL_SUPPORTED);
  }

  public PrimitiveApiDataTypeDef(
      ApiTypeName typeName,
      DataType cqlType,
      ApiSupportDef apiSupport,
      DefaultTypeBindingRules supportBindingRules) {
    this.typeName = typeName;
    this.cqlType = cqlType;
    this.apiSupport = Objects.requireNonNull(apiSupport, "apiSupport must not be null");
    this.supportBindingRules =
        Objects.requireNonNull(supportBindingRules, "bindingRules must not be null");
  }

  public DefaultTypeBindingRules supportBindingRules() {
    return supportBindingRules;
  }

  @Override
  public ApiTypeName typeName() {
    return typeName;
  }

  @Override
  public DataType cqlType() {
    return cqlType;
  }

  @Override
  public ApiSupportDef apiSupport() {
    return apiSupport;
  }

  @Override
  public ColumnDesc getSchemaDescription(SchemaDescBindingPoint bindingPoint) {
    // Always has same representation

    // Not easy to cache in the ctor because of the circular dependency
    // is only a cache lookup so not a big deal
    return PrimitiveColumnDesc.fromApiDataType(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PrimitiveApiDataTypeDef that = (PrimitiveApiDataTypeDef) o;
    return Objects.equals(typeName, that.typeName) && Objects.equals(cqlType, that.cqlType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(typeName, cqlType);
  }

  @Override
  public String toString() {
    return String.format("PrimitiveApiDataTypeDef{typeName=%s, cqlType=%s}", typeName, cqlType);
  }
}
