package io.stargate.sgv2.jsonapi.service.shredding;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.service.shredding.collections.JsonPath;
import java.util.Collection;

public class UnorderedJsonNamedValueContainer
    extends UnorderedNamedValueContainer<JsonPath, JsonLiteral<?>, JsonNamedValue>
    implements JsonNamedValueContainer {

  public UnorderedJsonNamedValueContainer() {
    super();
  }

  public UnorderedJsonNamedValueContainer(int initialCapacity) {
    super(initialCapacity);
  }

  public UnorderedJsonNamedValueContainer(JsonNamedValueContainer container) {
    super(container);
  }

  public UnorderedJsonNamedValueContainer(Collection<JsonNamedValue> values) {
    super();
    putAll(values);
  }
}
