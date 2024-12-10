package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.datastax.oss.driver.api.core.type.DataType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiSupportDef;

@JsonPropertyOrder({"createTable", "insert", "read", "filter", "cqlDefinition"})
public record ApiSupportDesc(
    boolean createTable, boolean insert, boolean read, boolean filter, String cqlDefinition) {

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
