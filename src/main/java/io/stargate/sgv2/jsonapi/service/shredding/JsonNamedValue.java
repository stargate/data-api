package io.stargate.sgv2.jsonapi.service.shredding;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.service.shredding.collections.JsonPath;

/**
 * A value that has come from Jackson or can be used to create a Jackson object.
 *
 * <p>The value is a {@link JsonLiteral} where the value is a Java object, not a Jackson node, which
 * is standard behaviour for the {@link JsonLiteral}.
 */
public class JsonNamedValue extends NamedValue<JsonPath, JsonLiteral<?>> {

  public JsonNamedValue(JsonPath name, JsonLiteral<?> value) {
    super(name, value);
  }

  @Override
  public String toString() {
    return new StringBuilder(getClass().getSimpleName())
        .append("{name=")
        .append(name())
        .append(", value=")
        .append(value())
        .append("}")
        .toString();
  }
}
