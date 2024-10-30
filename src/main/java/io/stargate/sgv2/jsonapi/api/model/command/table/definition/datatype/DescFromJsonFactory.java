package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

abstract class DescFromJsonFactory {

  protected static String typeNameOrMissing(String typeName) {
    return (typeName == null || typeName.isBlank()) ? "[MISSING]" : typeName;
  }
}
