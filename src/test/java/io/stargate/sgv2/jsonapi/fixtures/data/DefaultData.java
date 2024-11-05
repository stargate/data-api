package io.stargate.sgv2.jsonapi.fixtures.data;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.driver.internal.core.type.DefaultVectorType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.EJSONWrapper;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.fixtures.CqlFixture;
import io.stargate.sgv2.jsonapi.service.cqldriver.override.ExtendedVectorType;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowShredder;
import java.math.BigDecimal;
import java.util.Random;

/**
 * Implementations fo the {@link FixtureData} interface that provide test data for use by the {@link
 * CqlFixture}.
 *
 * <p>Add implementations as inner subclasses and add them to the {@link #SUPPORTED} list, so they
 * are picked up. Do not add unsupported datatyes to this list :) Just add it as a source and use it
 * as needed see {@link UnsupportedTypesData} for an example.
 *
 * <p>The name of the subclass is used in the test description.
 */
public class DefaultData implements FixtureData {

  private static final Random RANDOM = new Random();

  @Override
  public JsonLiteral<?> fromJSON(DataType type) {
    // We want to get the value type that the RowShredder will generate, so just call it directly
    return RowShredder.shredValue(getJsonNode(type));
  }

  protected JsonNode getJsonNode(DataType type) {
    // there is no easy way to do this, the DataType is an interface and the ENUM it has in
    // getProtocolCode()
    // is not always clear what it is, so we have to do value equality checks
    if (DataTypes.ASCII.equals(type)) {
      return JsonNodeFactory.instance.textNode("text");
    } else if (DataTypes.BOOLEAN.equals(type)) {
      return JsonNodeFactory.instance.booleanNode(true);
    } else if (DataTypes.BIGINT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(1L);
    } else if (DataTypes.BLOB.equals(type)) {
      return JsonNodeFactory.instance
          .objectNode()
          .put(EJSONWrapper.$BINARY, "aGVsbG8="); // hello in base 64
    } else if (DataTypes.DATE.equals(type)) {
      return JsonNodeFactory.instance.textNode("2020-01-01");
    } else if (DataTypes.DECIMAL.equals(type)) {
      return JsonNodeFactory.instance.numberNode(new BigDecimal("1.1"));
    } else if (DataTypes.DOUBLE.equals(type)) {
      return JsonNodeFactory.instance.numberNode(Double.parseDouble("1.1"));
    } else if (DataTypes.DURATION.equals(type)) {
      return JsonNodeFactory.instance.textNode("PT89H8M53S");
    } else if (DataTypes.FLOAT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(Float.parseFloat("1.1"));
    } else if (DataTypes.INET.equals(type)) {
      return JsonNodeFactory.instance.textNode("127.0.0.1");
    } else if (DataTypes.INT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(Integer.parseInt("1"));
    } else if (DataTypes.SMALLINT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(Short.parseShort("1"));
    } else if (DataTypes.TEXT.equals(type)) {
      return JsonNodeFactory.instance.textNode("text");
    } else if (DataTypes.TIME.equals(type)) {
      return JsonNodeFactory.instance.textNode("13:13:04.010");
    } else if (DataTypes.TIMESTAMP.equals(type)) {
      return JsonNodeFactory.instance.textNode("1970-02-01T13:13:04Z");
    } else if (DataTypes.TINYINT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(Byte.valueOf("1"));
    } else if (DataTypes.UUID.equals(type)) {
      return JsonNodeFactory.instance.textNode("550e8400-e29b-41d4-a716-446655440000");
    } else if (DataTypes.VARINT.equals(type)) {
      return JsonNodeFactory.instance.numberNode(BigDecimal.valueOf(1L));
    } else if (type.getClass().getName().startsWith(DefaultVectorType.VECTOR_CLASS_NAME)) {
      // See Driver class DataTypes#custom() for the above check
      return vectorNode((VectorType) type);
    } else if (type.getClass().getName().startsWith(ExtendedVectorType.class.getName())) {
      // See Driver class DataTypes#custom() for the above check
      return vectorNode((VectorType) type);
    }
    throw new UnsupportedOperationException(
        "Unknown type: %s className %s".formatted(type, type.getClass().getName()));
  }

  private JsonNode vectorNode(VectorType vt) {
    var arrayNode = JsonNodeFactory.instance.arrayNode(vt.getDimensions());
    for (int i = 0; i < vt.getDimensions(); i++) {
      arrayNode.add(RANDOM.nextFloat());
    }
    return arrayNode;
  }

  /** Override so the test description gets a simple name */
  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
