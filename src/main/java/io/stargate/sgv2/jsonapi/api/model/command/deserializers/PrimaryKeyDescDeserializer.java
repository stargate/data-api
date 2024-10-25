package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.PrimaryKeyDesc;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class PrimaryKeyDescDeserializer extends StdDeserializer<PrimaryKeyDesc> {
  protected PrimaryKeyDescDeserializer() {
    super(PrimaryKeyDescDeserializer.class);
  }

  @Override
  public PrimaryKeyDesc deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException, JacksonException {
    JsonNode primaryKey = deserializationContext.readTree(jsonParser);
    // This is primary key definition
    if (primaryKey.isTextual()) {
      return new PrimaryKeyDesc(new String[] {primaryKey.asText()}, null);
    }
    if (primaryKey.isObject()) {
      String[] keys = null;
      PrimaryKeyDesc.OrderingKeyDesc[] orderingKeyDescs = null;
      if (primaryKey.has("partitionBy")) {
        JsonNode partitionBy = primaryKey.path("partitionBy");

        if (partitionBy.isArray()) {
          keys = new String[partitionBy.size()];
          for (int i = 0; i < partitionBy.size(); i++) {
            keys[i] = partitionBy.get(i).asText();
          }

        } else {
          throw SchemaException.Code.PRIMARY_KEY_DEFINITION_INCORRECT.get();
        }

        if (primaryKey.has("partitionSort")) {
          JsonNode partitionSort = primaryKey.path("partitionSort");
          if (partitionSort.isObject()) {
            orderingKeyDescs = new PrimaryKeyDesc.OrderingKeyDesc[partitionSort.size()];
            int i = 0;
            final Iterator<Map.Entry<String, JsonNode>> orderingKeysData = partitionSort.fields();
            while (orderingKeysData.hasNext()) {
              Map.Entry<String, JsonNode> entry = orderingKeysData.next();
              String columnName = entry.getKey();
              if (entry.getValue().isInt()) {
                int order = entry.getValue().asInt();
                if (order == 1) {
                  orderingKeyDescs[i] =
                      new PrimaryKeyDesc.OrderingKeyDesc(
                          columnName, PrimaryKeyDesc.OrderingKeyDesc.Order.ASC);
                } else if (order == -1) {
                  orderingKeyDescs[i] =
                      new PrimaryKeyDesc.OrderingKeyDesc(
                          columnName, PrimaryKeyDesc.OrderingKeyDesc.Order.DESC);
                } else {
                  throw SchemaException.Code.PRIMARY_KEY_DEFINITION_INCORRECT.get();
                }
              } else {
                throw SchemaException.Code.PRIMARY_KEY_DEFINITION_INCORRECT.get();
              }
              i++;
            }
          }
        }
        return new PrimaryKeyDesc(keys, orderingKeyDescs);
      } else {
        throw SchemaException.Code.PRIMARY_KEY_DEFINITION_INCORRECT.get();
      }
    }
    throw SchemaException.Code.PRIMARY_KEY_DEFINITION_INCORRECT.get();
  }
}
