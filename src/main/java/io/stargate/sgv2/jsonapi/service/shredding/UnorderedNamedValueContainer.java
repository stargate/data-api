package io.stargate.sgv2.jsonapi.service.shredding;

import java.util.Collection;
import java.util.HashMap;

public class UnorderedNamedValueContainer<NameT, ValueT, NvT extends NamedValue<NameT, ValueT>>
    extends HashMap<NameT, NvT> implements NamedValueContainer<NameT, ValueT, NvT> {

  public UnorderedNamedValueContainer() {
    super();
  }

  public UnorderedNamedValueContainer(int initialCapacity) {
    super(initialCapacity);
  }

  public UnorderedNamedValueContainer(NamedValueContainer<NameT, ValueT, NvT> container) {
    super(container);
  }

  public UnorderedNamedValueContainer(Collection<NvT> values) {
    super();
    putAll(values);
  }
}
