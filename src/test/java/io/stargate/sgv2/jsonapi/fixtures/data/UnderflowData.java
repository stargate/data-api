package io.stargate.sgv2.jsonapi.fixtures.data;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Numerics with a value that is too small, keep in sync with
 * {@link io.stargate.sgv2.jsonapi.fixtures.types.CqlTypesForTesting#UNDERFLOW_TYPES}
 */
public class UnderflowData extends DefaultData{

  private static final BigDecimal DOUBLE_UNDERFLOW = BigDecimal.valueOf(Double.MIN_VALUE).subtract(BigDecimal.valueOf(Double.MAX_VALUE));
  private static final BigInteger INT_UNDERFLOW = BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.valueOf(Long.MAX_VALUE));
  @Override
  protected JsonNode getJsonNode(DataType type) {

    if (DataTypes.DOUBLE.equals(type)) {
      return JsonNodeFactory.instance.numberNode(DOUBLE_UNDERFLOW);
    } else if (DataTypes.FLOAT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(DOUBLE_UNDERFLOW);
    } else if (DataTypes.INT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(INT_UNDERFLOW);
    } else if (DataTypes.SMALLINT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(INT_UNDERFLOW);
    } else if (DataTypes.TINYINT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(INT_UNDERFLOW);
    }
    return super.getJsonNode(type);
  }
}
