package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.PrimaryKey;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class PrimaryKeyDeserializer extends StdDeserializer<PrimaryKey> {
  protected PrimaryKeyDeserializer() {
    super(PrimaryKeyDeserializer.class);
  }

  @Override
  public PrimaryKey deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException, JacksonException {
    JsonNode primaryKey = deserializationContext.readTree(jsonParser);
    // This is primary key definition
    if (primaryKey.isTextual()) {
      return new PrimaryKey(new String[] {primaryKey.asText()}, null);
    }
    if (primaryKey.isObject()) {
      String[] keys = null;
      PrimaryKey.OrderingKey[] orderingKeys = null;
      if (primaryKey.has("partitionBy")) {
        JsonNode partitionBy = primaryKey.path("partitionBy");

        if (partitionBy.isArray()) {
          keys = new String[partitionBy.size()];
          for (int i = 0; i < partitionBy.size(); i++) {
            keys[i] = partitionBy.get(i).asText();
          }

        } else {
          throw ErrorCode.TABLE_PRIMARY_KEY_DEFINITION_INCORRECT.toApiException(
              "partitionBy needs to be array of column names");
        }

        if (primaryKey.has("partitionSort")) {
          JsonNode partitionSort = primaryKey.path("partitionSort");
          if (partitionSort.isObject()) {
            orderingKeys = new PrimaryKey.OrderingKey[partitionSort.size()];
            int i = 0;
            final Iterator<Map.Entry<String, JsonNode>> orderingKeysData = partitionSort.fields();
            while (orderingKeysData.hasNext()) {
              Map.Entry<String, JsonNode> entry = orderingKeysData.next();
              String columnName = entry.getKey();
              if (entry.getValue().isInt()) {
                int order = entry.getValue().asInt();
                if (order == 1) {
                  orderingKeys[i] =
                      new PrimaryKey.OrderingKey(columnName, PrimaryKey.OrderingKey.Order.ASC);
                } else if (order == -1) {
                  orderingKeys[i] =
                      new PrimaryKey.OrderingKey(columnName, PrimaryKey.OrderingKey.Order.DESC);
                } else {
                  throw ErrorCode.TABLE_PRIMARY_KEY_DEFINITION_INCORRECT.toApiException(
                      "partitionSort value should be 1 or -1");
                }
              } else {
                throw ErrorCode.TABLE_PRIMARY_KEY_DEFINITION_INCORRECT.toApiException(
                    "partitionSort should be 1 or -1");
              }
              i++;
            }
          }
        }
        return new PrimaryKey(keys, orderingKeys);
      } else {
        throw ErrorCode.TABLE_PRIMARY_KEY_DEFINITION_INCORRECT.toApiException(
            "partitionBy keys is mandatory");
      }
    }
    throw ErrorCode.TABLE_PRIMARY_KEY_DEFINITION_INCORRECT.toApiException(
        "Primary key should be text or Object");
  }
}