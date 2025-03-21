package io.stargate.sgv2.jsonapi.service.shredding;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import java.util.Collection;

/** A container for {@link CqlNamedValue}'s that maintains the order the named values were added. */
public class CqlNamedValueContainer
    extends NamedValueContainer<CqlIdentifier, Object, CqlNamedValue> {

  public CqlNamedValueContainer() {
    super();
  }

  public CqlNamedValueContainer(int initialCapacity) {
    super(initialCapacity);
  }

  public CqlNamedValueContainer(CqlNamedValueContainer container) {
    super(container);
  }

  public CqlNamedValueContainer(Collection<CqlNamedValue> values) {
    super(values.size());
    putAll(values);
  }
}
