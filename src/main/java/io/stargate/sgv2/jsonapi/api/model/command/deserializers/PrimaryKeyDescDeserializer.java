package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.PrimaryKeyDesc;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import java.io.IOException;

public class PrimaryKeyDescDeserializer extends StdDeserializer<PrimaryKeyDesc> {

  private static final String ERR_PREFIX =
      "The Long Form %s definition".formatted(TableDescConstants.TableDefinitionDesc.PRIMARY_KEY);
  private static final String ERR_PARTITION_BY_ARRAY =
      ERR_PREFIX
          + " must have a %s field that is a JSON Array of Strings"
              .formatted(TableDescConstants.PrimaryKey.PARTITION_BY);
  private static final String ERR_PARTITION_SORT_OBJECT =
      ERR_PREFIX
          + " may have a %s field that is a JSON Object, each field is the name of a column, with a value of %s for %s, or %s for %s"
              .formatted(
                  TableDescConstants.PrimaryKey.PARTITION_SORT,
                  PrimaryKeyDesc.OrderingKeyDesc.Order.ASC.ordinal,
                  PrimaryKeyDesc.OrderingKeyDesc.Order.ASC.name(),
                  PrimaryKeyDesc.OrderingKeyDesc.Order.DESC.ordinal,
                  PrimaryKeyDesc.OrderingKeyDesc.Order.DESC.name());

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

    if (!primaryKey.isObject()) {
      throw new JsonMappingException(
          jsonParser,
          "%s definition must be a JSON String or Object"
              .formatted(TableDescConstants.TableDefinitionDesc.PRIMARY_KEY));
    }

    var partitionByNode = primaryKey.path(TableDescConstants.PrimaryKey.PARTITION_BY);
    if (partitionByNode.isMissingNode()) {
      throw new JsonMappingException(jsonParser, ERR_PARTITION_BY_ARRAY  + " (node is missing)");
    }
    if (!partitionByNode.isArray()) {
      throw new JsonMappingException(jsonParser, ERR_PARTITION_BY_ARRAY  + " (node is not Array)");
    }

    String[] keys = new String[partitionByNode.size()];
    for (int i = 0; i < partitionByNode.size(); i++) {
      if (!partitionByNode.get(i).isTextual()) {
        throw new JsonMappingException(jsonParser, ERR_PARTITION_BY_ARRAY + " (array element is not String)");
      }
      keys[i] = partitionByNode.get(i).asText();
    }

    PrimaryKeyDesc.OrderingKeyDesc[] orderingKeyDescs = null;
    var partitionSortNode = primaryKey.path(TableDescConstants.PrimaryKey.PARTITION_SORT);
    if (!partitionSortNode.isMissingNode()) {
      if (!partitionSortNode.isObject()) {
        throw new JsonMappingException(jsonParser, ERR_PARTITION_SORT_OBJECT + " (node is not Object)");
      }

      // each field in the partitionSortNode is a column name and the value is the order
      orderingKeyDescs = new PrimaryKeyDesc.OrderingKeyDesc[partitionSortNode.size()];
      int i = 0;
      for (var sortFieldEntry : partitionSortNode.properties()) {
        String columnName = sortFieldEntry.getKey();
        if (!sortFieldEntry.getValue().isInt()) {
          throw new JsonMappingException(jsonParser, ERR_PARTITION_SORT_OBJECT  + " (field is not Integer)");
        }

        var order =
            PrimaryKeyDesc.OrderingKeyDesc.Order.fromUserDesc(sortFieldEntry.getValue().asInt())
                .orElseThrow(() -> new JsonMappingException(jsonParser, ERR_PARTITION_SORT_OBJECT  + " (order is not recognized)"));

        orderingKeyDescs[i++] = new PrimaryKeyDesc.OrderingKeyDesc(columnName, order);
      }
    }
    return new PrimaryKeyDesc(keys, orderingKeyDescs);
  }
}
