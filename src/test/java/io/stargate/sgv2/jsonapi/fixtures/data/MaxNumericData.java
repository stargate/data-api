package io.stargate.sgv2.jsonapi.fixtures.data;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.math.BigDecimal;

/** Numerics with the max value */
public class MaxNumericData extends DefaultData {

  @Override
  protected JsonNode getJsonNode(DataType type) {
    if (DataTypes.BIGINT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(Long.MAX_VALUE);
    } else if (DataTypes.DECIMAL.equals(type)) {
      // there is no max, use max for double
      return JsonNodeFactory.instance.numberNode(BigDecimal.valueOf(Double.MAX_VALUE));
    } else if (DataTypes.DOUBLE.equals(type)) {
      return JsonNodeFactory.instance.numberNode(Double.MAX_VALUE);
    } else if (DataTypes.FLOAT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(Float.MAX_VALUE);
    } else if (DataTypes.INT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(Integer.MAX_VALUE);
    } else if (DataTypes.SMALLINT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(Short.MAX_VALUE);
    } else if (DataTypes.TINYINT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(Byte.MAX_VALUE);
    } else if (DataTypes.VARINT.equals(type)) {
      // No max, use max Long
      return JsonNodeFactory.instance.numberNode(BigDecimal.valueOf(Long.MAX_VALUE));
    }
    return super.getJsonNode(type);
  }
}
