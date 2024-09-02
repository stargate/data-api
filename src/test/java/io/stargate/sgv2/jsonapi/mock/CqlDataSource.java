package io.stargate.sgv2.jsonapi.mock;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowShredder;
import java.math.BigDecimal;
import java.util.List;

/**
 * Implementations fo the {@link CqlData} interface that provide test data for use by the {@link
 * CqlFixture}.
 *
 * <p>Add implementations as inner subclasses and add them to the {@link #ALL_SOURCES} list, so they
 * are picked up.
 *
 * <p>The name of the subclass is used in the test description.
 */
public abstract class CqlDataSource implements CqlData {

  public static final List<CqlData> ALL_SOURCES;

  static {
    ALL_SOURCES = List.of(new DefaultData(), new MaxNumericData(), new MinNumericData());
  }

  @Override
  public Object fromJSON(DataType type) {
    // We want to get the value type that the RowShredder will generate, so just call it directly
    return RowShredder.shredValue(getJsonNode(type));
  }

  protected JsonNode getJsonNode(DataType type) {
    // there is no easy way to do this, the DataType is an interface and the ENUM it has in
    // getProtocolCode()
    // is not always clear what it is, so we have to do value equality checks
    if (DataTypes.TEXT.equals(type)) {
      return JsonNodeFactory.instance.textNode("text");
    } else if (DataTypes.BOOLEAN.equals(type)) {
      return JsonNodeFactory.instance.booleanNode(true);
    } else if (DataTypes.BIGINT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(1L);
    } else if (DataTypes.DECIMAL.equals(type)) {
      return JsonNodeFactory.instance.numberNode(new BigDecimal("1.1"));
    } else if (DataTypes.DOUBLE.equals(type)) {
      return JsonNodeFactory.instance.numberNode(Double.parseDouble("1.1"));
    } else if (DataTypes.FLOAT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(Float.parseFloat("1.1"));
    } else if (DataTypes.INT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(Integer.parseInt("1"));
    } else if (DataTypes.SMALLINT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(Short.parseShort("1"));
    } else if (DataTypes.TINYINT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(Byte.valueOf("1"));
    } else if (DataTypes.VARINT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(BigDecimal.valueOf(1L));
    }
    throw new UnsupportedOperationException("Unknown type: " + type);
  }

  /** Override so the test description gets a simple name */
  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  /** Basic values for all types */
  private static class DefaultData extends CqlDataSource {}

  /** Numerics with the max value */
  private static class MaxNumericData extends CqlDataSource {
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

  /** Numerics with the min value */
  private static class MinNumericData extends CqlDataSource {
    @Override
    protected JsonNode getJsonNode(DataType type) {
      if (DataTypes.BIGINT.equals(type)) {
        return JsonNodeFactory.instance.numberNode(Long.MIN_VALUE);
      } else if (DataTypes.DECIMAL.equals(type)) {
        // there is no max, use max for double
        return JsonNodeFactory.instance.numberNode(BigDecimal.valueOf(Double.MIN_VALUE));
      } else if (DataTypes.DOUBLE.equals(type)) {
        return JsonNodeFactory.instance.numberNode(Double.MIN_VALUE);
      } else if (DataTypes.FLOAT.equals(type)) {
        return JsonNodeFactory.instance.numberNode(Float.MIN_VALUE);
      } else if (DataTypes.INT.equals(type)) {
        return JsonNodeFactory.instance.numberNode(Integer.MIN_VALUE);
      } else if (DataTypes.SMALLINT.equals(type)) {
        return JsonNodeFactory.instance.numberNode(Short.MIN_VALUE);
      } else if (DataTypes.TINYINT.equals(type)) {
        return JsonNodeFactory.instance.numberNode(Byte.MIN_VALUE);
      } else if (DataTypes.VARINT.equals(type)) {
        // No max, use max Long
        return JsonNodeFactory.instance.numberNode(BigDecimal.valueOf(Long.MIN_VALUE));
      }
      return super.getJsonNode(type);
    }
  }
}
