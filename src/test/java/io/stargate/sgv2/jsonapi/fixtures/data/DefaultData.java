package io.stargate.sgv2.jsonapi.fixtures.data;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.fixtures.CqlFixture;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowShredder;
import java.math.BigDecimal;

/**
 * Implementations fo the {@link FixtureData} interface that provide test data for use by the {@link
 * CqlFixture}.
 *
 * <p>Add implementations as inner subclasses and add them to the {@link #SUPPORTED} list,
 * so they are picked up. Do not add unsupported datatyes to this list :) Just add it as a source
 * and use it as needed see {@link UnsupportedTypesData} for an example.
 *
 * <p>The name of the subclass is used in the test description.
 */
public class DefaultData implements FixtureData {


  @Override
  public JsonLiteral<?> fromJSON(DataType type) {
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
}
