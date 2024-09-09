package io.stargate.sgv2.jsonapi.service.shredding;

import java.util.Collection;
import java.util.LinkedHashMap;

/**
 * Base implementation for a {@link NamedValueContainer} that maintains the order the named values
 * were added.
 */
public class OrderedNamedValueContainer<NameT, ValueT, NvT extends NamedValue<NameT, ValueT>>
    extends LinkedHashMap<NameT, NvT> implements NamedValueContainer<NameT, ValueT, NvT> {

  public OrderedNamedValueContainer() {
    super();
  }

  public OrderedNamedValueContainer(int initialCapacity) {
    super(initialCapacity);
  }

  public OrderedNamedValueContainer(NamedValueContainer<NameT, ValueT, NvT> container) {
    super(container);
  }

  public OrderedNamedValueContainer(Collection<NvT> values) {
    super();
    putAll(values);
  }
}
