package io.stargate.sgv2.jsonapi.fixtures.data;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.fixtures.types.CqlTypesForTesting;

/**
 * Returns data for the unsupported types, and then falls back to the default for the supported Kept
 * out of the regular list of testing, so it can be used as needed.
 *
 * <p>Is in sync with {@link CqlTypesForTesting#UNSUPPORTED_FOR_INSERT}
 */
public class UnsupportedTypesData extends DefaultData {
  @Override
  protected JsonNode getJsonNode(DataType type) {
    if (DataTypes.COUNTER.equals(type)) {
      return JsonNodeFactory.instance.numberNode(1L);
    } else if (DataTypes.listOf(DataTypes.INT).equals(type)) {
      return JsonNodeFactory.instance.arrayNode().add(1);
    } else if (DataTypes.listOf(DataTypes.INT, true).equals(type)) {
      return JsonNodeFactory.instance.arrayNode().add(1);
    } else if (DataTypes.setOf(DataTypes.TEXT).equals(type)) {
      return JsonNodeFactory.instance.arrayNode().add("text");
    } else if (DataTypes.setOf(DataTypes.TEXT, true).equals(type)) {
      return JsonNodeFactory.instance.arrayNode().add("text");
    } else if (DataTypes.mapOf(DataTypes.TEXT, DataTypes.DOUBLE).equals(type)) {
      return JsonNodeFactory.instance.objectNode().put("text", 1.1);
    } else if (DataTypes.mapOf(DataTypes.TEXT, DataTypes.DOUBLE, true).equals(type)) {
      return JsonNodeFactory.instance.objectNode().put("text", 1.1);
    }
    return super.getJsonNode(type);
  }
}
