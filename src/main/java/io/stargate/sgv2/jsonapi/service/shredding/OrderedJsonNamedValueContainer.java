package io.stargate.sgv2.jsonapi.service.shredding;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.service.shredding.collections.JsonPath;
import java.util.Collection;

public class OrderedJsonNamedValueContainer
    extends OrderedNamedValueContainer<JsonPath, JsonLiteral<?>, JsonNamedValue>
    implements JsonNamedValueContainer {

  public OrderedJsonNamedValueContainer() {
    super();
  }

  public OrderedJsonNamedValueContainer(int initialCapacity) {
    super(initialCapacity);
  }

  public OrderedJsonNamedValueContainer(JsonNamedValueContainer container) {
    super(container);
  }

  public OrderedJsonNamedValueContainer(Collection<JsonNamedValue> values) {
    super();
    putAll(values);
  }
}
