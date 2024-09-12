package io.stargate.sgv2.jsonapi.fixtures.data;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/** Generates integer values the types that support reals types, e.g. "1" for a float. */
public class IntegersForRealData extends DefaultData {

  private static final JsonNode EXACTLY_ONE = JsonNodeFactory.instance.numberNode(1);

  @Override
  protected JsonNode getJsonNode(DataType type) {

    if (DataTypes.DECIMAL.equals(type)) {
      return EXACTLY_ONE;
    } else if (DataTypes.DOUBLE.equals(type)) {
      return EXACTLY_ONE;
    } else if (DataTypes.FLOAT.equals(type)) {
      return EXACTLY_ONE;
    }
    return super.getJsonNode(type);
  }
}
