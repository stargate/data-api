package io.stargate.sgv2.jsonapi.fixtures.data;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Numerics with a value that is too large, keep in sync with
 * {@link io.stargate.sgv2.jsonapi.fixtures.types.CqlTypesForTesting#OVERFLOW_TYPES}
 */
public class OverflowData extends DefaultData{

  private static final BigDecimal DOUBLE_OVERFLOW = BigDecimal.valueOf(Double.MAX_VALUE).multiply(BigDecimal.valueOf(2));
  private static final BigInteger INT_OVERFLOW = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(2));
  @Override
  protected JsonNode getJsonNode(DataType type) {

    if (DataTypes.DOUBLE.equals(type)) {
      return JsonNodeFactory.instance.numberNode(DOUBLE_OVERFLOW);
    } else if (DataTypes.FLOAT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(DOUBLE_OVERFLOW);
    } else if (DataTypes.INT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(INT_OVERFLOW);
    } else if (DataTypes.SMALLINT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(INT_OVERFLOW);
    } else if (DataTypes.TINYINT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(INT_OVERFLOW);
    }
    return super.getJsonNode(type);
  }
}
