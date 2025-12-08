package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescSource;

abstract class ColumnDescFromJsonFactory<T extends ColumnDesc> {

  public abstract T create(
      String columnName,
      SchemaDescSource schemaDescSource,
      JsonParser jsonParser,
      JsonNode columnDescNode)
      throws JsonProcessingException;
}
