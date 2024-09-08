package io.stargate.sgv2.jsonapi.service.shredding;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.service.shredding.collections.JsonPath;

public class JsonNamedValue extends NamedValue<JsonPath, JsonLiteral<?>> {

  public JsonNamedValue(JsonPath name, JsonLiteral<?> value) {
    super(name, value);
  }

  @Override
  public String toString() {
    return new StringBuilder(getClass().getSimpleName())
        .append("{name=").append(name())
        .append(", value=").append(value())
        .append("}")
        .toString();
  }
}
