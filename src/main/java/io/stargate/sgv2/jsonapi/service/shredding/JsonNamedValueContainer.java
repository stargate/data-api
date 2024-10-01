package io.stargate.sgv2.jsonapi.service.shredding;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.service.shredding.collections.JsonPath;
import java.util.Collection;

/** A container for {@link JsonNamedValue} that maintains the order the named values were added. */
public class JsonNamedValueContainer
    extends NamedValueContainer<JsonPath, JsonLiteral<?>, JsonNamedValue> {

  public JsonNamedValueContainer() {
    super();
  }

  public JsonNamedValueContainer(int initialCapacity) {
    super(initialCapacity);
  }

  public JsonNamedValueContainer(JsonNamedValueContainer container) {
    super(container);
  }

  public JsonNamedValueContainer(Collection<JsonNamedValue> values) {
    super();
    putAll(values);
  }
}
