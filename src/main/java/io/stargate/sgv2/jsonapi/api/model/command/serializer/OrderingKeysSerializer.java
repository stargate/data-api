package io.stargate.sgv2.jsonapi.api.model.command.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.PrimaryKey;
import java.io.IOException;

/**
 * Custom serializer to encode the column type to the JSON payload This is required because
 * composite and custom column types may need additional properties to be serialized
 */
public class OrderingKeysSerializer extends JsonSerializer<PrimaryKey.OrderingKey[]> {

  @Override
  public void serialize(
      PrimaryKey.OrderingKey[] orderingKeys,
      JsonGenerator jsonGenerator,
      SerializerProvider serializerProvider)
      throws IOException {
    jsonGenerator.writeStartObject();
    if (orderingKeys != null) {
      for (PrimaryKey.OrderingKey orderingKey : orderingKeys) {
        jsonGenerator.writeNumberField(
            orderingKey.column(), orderingKey.order() == PrimaryKey.OrderingKey.Order.ASC ? 1 : -1);
      }
    }
    jsonGenerator.writeEndObject();
  }

  /**
   * This is used when a unsupported type column is present in a table. How to use this class will
   * evolve as different unsupported types are analyzed.
   *
   * @param createTable
   * @param insert
   * @param read
   * @param cqlDefinition
   */
  private record ApiSupport(
      boolean createTable, boolean insert, boolean read, String cqlDefinition) {}
}
