package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

public record ApiSupportDesc(
    boolean createTable, boolean insert, boolean read, String cqlDefinition) {

  public static ApiSupportDesc fullSupport(String cqlDefinition) {
    return new ApiSupportDesc(true, true, true, cqlDefinition);
  }

  public static ApiSupportDesc noSupport(String cqlDefinition) {
    return new ApiSupportDesc(false, false, false, cqlDefinition);
  }
}
