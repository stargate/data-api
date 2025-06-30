package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.fasterxml.jackson.databind.JsonNode;

abstract class DescFromJsonFactory {

  protected static String typeNameOrMissing(JsonNode typeName) {
    return (typeName == null || (typeName.isTextual() && typeName.textValue().isBlank()))
        ? "[MISSING]"
        : typeName.toString();
  }
}
