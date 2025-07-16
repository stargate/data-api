package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.datastax.oss.driver.api.core.type.DataType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiSupportDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description of the API support for a data type to be used in the public API with users via the
 * {@link ColumnDesc#apiSupport()}.
 *
 * <p>They are normally created from the {@link ApiDataType} , see {@link ApiDataType#apiSupport()}
 * and {@link ApiDataType#columnDesc()} for more information.
 */
@JsonPropertyOrder({"createTable", "insert", "read", "filter", "cqlDefinition"})
public record ApiSupportDesc(
    boolean createTable, boolean insert, boolean read, boolean filter, String cqlDefinition) {
  private static final Logger LOGGER = LoggerFactory.getLogger(ApiSupportDesc.class);

  @JsonIgnore
  public boolean isAnyUnsupported() {
    return !createTable || !insert || !read || !filter;
  }

  /**
   * Create an instance of without knowing the CQL definition.
   *
   * <p>For use when creating from an API request.
   */
  public static ApiSupportDesc withoutCqlDefinition(ApiSupportDef apiSupportDef) {
    return from(apiSupportDef, "UNAVAILABLE");
  }

  /** Create an instance of from an API data type. */
  public static ApiSupportDesc from(ApiDataType apiDataType) {
    return from(apiDataType.apiSupport(), apiDataType.cqlType());
  }

  /** Create an instance of from an API support definition and a CQL type. */
  public static ApiSupportDesc from(ApiSupportDef apiSupportDef, DataType cqlType) {
    return from(apiSupportDef, cqlType.asCql(true, true));
  }

  private static ApiSupportDesc from(ApiSupportDef apiSupportDef, String cqlDefinition) {
    return new ApiSupportDesc(
        apiSupportDef.createTable(),
        apiSupportDef.insert(),
        apiSupportDef.read(),
        apiSupportDef.filter(),
        cqlDefinition);
  }
}
